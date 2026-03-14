// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.

package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"kbn-ts-type-check-oblt-server-go/internal/ingestion"
	"kbn-ts-type-check-oblt-server-go/internal/source"
	"kbn-ts-type-check-oblt-server-go/internal/storage"
)

type handler struct {
	store storage.ArtifactStore
	src   *source.Config
	log   Logger
}

type artifactsRequest struct {
	CommitSha string   `json:"commitSha"`
	Projects  []string `json:"projects"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	data, _ := json.Marshal(v)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(data) //nolint:errcheck
}

func handleGetHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status":  "ok",
		"service": "kbn-ts-type-check-oblt-server",
	})
}

func (h *handler) handlePostArtifacts(w http.ResponseWriter, r *http.Request) {
	var body artifactsRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}

	if body.CommitSha == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing or invalid commitSha"})
		return
	}

	h.log.Info(fmt.Sprintf("POST /artifacts body: commitSha=%s projects=%d", body.CommitSha[:min(12, len(body.CommitSha))], len(body.Projects)))

	ctx := r.Context()

	index, err := h.store.ReadIndex(ctx, body.CommitSha)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	// Branch 1: No index found — GCS passthrough + async on-demand ingest.
	if index == nil {
		h.serveGCSPassthroughWithIngest(ctx, w, body.CommitSha)
		return
	}

	// Branch 2: Index exists but no project filter — full GCS passthrough (fastest path).
	if len(body.Projects) == 0 {
		h.log.Info(fmt.Sprintf("artifacts requested: commit %s, all projects — using GCS passthrough",
			body.CommitSha[:min(12, len(body.CommitSha))]))
		h.serveGCSPassthrough(ctx, w, body.CommitSha)
		return
	}

	// Branch 3: Selective restore — stream individual project blobs with length-prefix framing.
	//
	// Protocol: Content-Type: application/x-artifact-stream
	//   Repeat for each unique project blob:
	//     [4 bytes big-endian uint32: blob length][N bytes: raw .tar.gz blob]
	//
	// The server reads each pre-baked .tar.gz directly from disk with zero
	// CPU work (no decompress/recompress). The client extracts each small
	// archive immediately as it arrives, naturally pipelining download and
	// extraction without any buffering.
	h.log.Info(fmt.Sprintf("artifacts requested: commit %s, %d project(s) — framed stream",
		body.CommitSha[:min(12, len(body.CommitSha))], len(body.Projects)))

	// blobRef pairs a project path with its content hash so ReadArtifact can
	// locate the blob in the project-scoped directory layout.
	type blobRef struct {
		project string
		hash    string
	}

	// Deduplicate hashes while preserving the order of the requested project list.
	seen := make(map[string]struct{})
	var ordered []blobRef
	for _, p := range body.Projects {
		if hash, ok := index[p]; ok {
			if _, already := seen[hash]; !already {
				seen[hash] = struct{}{}
				ordered = append(ordered, blobRef{project: p, hash: hash})
			}
		}
	}

	w.Header().Set("Content-Type", "application/x-artifact-stream")
	w.Header().Set("X-Project-Count", strconv.Itoa(len(ordered)))
	w.WriteHeader(http.StatusOK)

	flusher, canFlush := w.(http.Flusher)

	for _, ref := range ordered {
		blob, err := h.store.ReadArtifact(ctx, ref.hash, ref.project)
		if err != nil {
			h.log.Warn(fmt.Sprintf("read artifact %s: %v", ref.hash[:min(12, len(ref.hash))], err))
			return
		}

		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(blob)))

		if _, err := w.Write(lenBuf[:]); err != nil {
			return
		}
		if _, err := w.Write(blob); err != nil {
			return
		}
		// Flush after each blob so the client can start extracting immediately
		// while the next blob is being read from disk.
		if canFlush {
			flusher.Flush()
		}
	}
}

// serveGCSPassthrough streams the full archive from GCS directly to the client.
func (h *handler) serveGCSPassthrough(ctx context.Context, w http.ResponseWriter, commitSha string) {
	archive, err := source.DownloadArchiveStream(h.src, commitSha)
	if err != nil {
		h.log.Warn(fmt.Sprintf("GCS passthrough failed for %s: %v", commitSha[:min(12, len(commitSha))], err))
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
		return
	}
	defer archive.Body.Close()

	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="artifacts-%s.tar.gz"`, commitSha[:min(12, len(commitSha))]))
	if archive.ContentLength > 0 {
		w.Header().Set("Content-Length", strconv.FormatInt(archive.ContentLength, 10))
	}
	w.WriteHeader(http.StatusOK)

	if _, err := io.Copy(w, archive.Body); err != nil {
		h.log.Warn(fmt.Sprintf("GCS passthrough write error for %s: %v", commitSha[:min(12, len(commitSha))], err))
	}
}

// serveGCSPassthroughWithIngest streams the archive from GCS to the client while
// concurrently capturing the bytes for asynchronous on-demand ingestion.
func (h *handler) serveGCSPassthroughWithIngest(ctx context.Context, w http.ResponseWriter, commitSha string) {
	h.log.Info(fmt.Sprintf("no index for commit %s, trying GCS source", commitSha[:min(12, len(commitSha))]))

	archive, err := source.DownloadArchiveStream(h.src, commitSha)
	if err != nil {
		h.log.Warn(fmt.Sprintf("GCS fallback failed for %s: %v", commitSha[:min(12, len(commitSha))], err))
		writeJSON(w, http.StatusNotFound, map[string]string{
			"error": fmt.Sprintf("no index found for commit %s", commitSha),
		})
		return
	}
	defer archive.Body.Close()

	h.log.Info(fmt.Sprintf("passthrough streaming archive from GCS for commit %s", commitSha[:min(12, len(commitSha))]))

	// Tee the GCS response body: stream to the client and capture bytes for ingestion.
	var buf bytes.Buffer
	tee := io.TeeReader(archive.Body, &buf)

	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="artifacts-%s.tar.gz"`, commitSha[:min(12, len(commitSha))]))
	if archive.ContentLength > 0 {
		w.Header().Set("Content-Length", strconv.FormatInt(archive.ContentLength, 10))
	}
	w.WriteHeader(http.StatusOK)

	if _, err := io.Copy(w, tee); err != nil {
		h.log.Warn(fmt.Sprintf("GCS tee write error for %s: %v", commitSha[:min(12, len(commitSha))], err))
		return
	}

	// Copy the captured bytes before handing them to the goroutine.
	archiveData := make([]byte, buf.Len())
	copy(archiveData, buf.Bytes())

	go h.ingestAsync(archiveData, commitSha)
}

// ingestAsync processes a captured archive in the background after the client response
// has been fully sent. Uses a fresh background context so it outlives the request.
func (h *handler) ingestAsync(archiveData []byte, commitSha string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	index, err := ingestion.TransformAndStore(ctx, archiveData, commitSha, h.store, &loggerAdapter{h.log})
	if err != nil {
		h.log.Warn(fmt.Sprintf("on-demand ingest failed for %s: %v", commitSha[:min(12, len(commitSha))], err))
		return
	}

	if err := h.store.WriteIndex(ctx, commitSha, index); err != nil {
		h.log.Warn(fmt.Sprintf("write index failed for %s: %v", commitSha[:min(12, len(commitSha))], err))
		return
	}

	cursor, err := h.store.ReadCursor(ctx)
	if err != nil {
		h.log.Warn(fmt.Sprintf("read cursor failed after ingest of %s: %v", commitSha[:min(12, len(commitSha))], err))
		return
	}

	processed := make(map[string]struct{})
	if cursor != nil {
		for _, sha := range cursor.ProcessedCommits {
			processed[sha] = struct{}{}
		}
	}
	processed[commitSha] = struct{}{}

	shas := make([]string, 0, len(processed))
	for sha := range processed {
		shas = append(shas, sha)
	}

	if err := h.store.WriteCursor(ctx, &storage.IngestionCursor{
		ProcessedCommits: shas,
		LastPollAt:       time.Now().UTC().Format(time.RFC3339),
	}); err != nil {
		h.log.Warn(fmt.Sprintf("write cursor failed after ingest of %s: %v", commitSha[:min(12, len(commitSha))], err))
		return
	}

	h.log.Info(fmt.Sprintf("ingested commit %s (on-demand)", commitSha[:min(12, len(commitSha))]))
}

// loggerAdapter adapts the server Logger interface to the ingestion.Logger interface.
type loggerAdapter struct {
	l Logger
}

func (a *loggerAdapter) Info(msg string) { a.l.Info(msg) }
func (a *loggerAdapter) Warn(msg string) { a.l.Warn(msg) }
