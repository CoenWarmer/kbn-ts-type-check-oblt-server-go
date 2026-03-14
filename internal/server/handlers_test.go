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
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"kbn-ts-type-check-oblt-server-go/internal/source"
	"kbn-ts-type-check-oblt-server-go/internal/storage"
)

// memStore is a minimal in-memory ArtifactStore used in handler tests.
type memStore struct {
	index     storage.CommitIndex
	artifacts map[string][]byte // key: hash+":"+project
}

func (m *memStore) ReadIndex(_ context.Context, _ string) (storage.CommitIndex, error) {
	return m.index, nil
}
func (m *memStore) WriteIndex(_ context.Context, _ string, _ storage.CommitIndex) error { return nil }
func (m *memStore) HasArtifact(_ context.Context, hash, project string) (bool, error) {
	_, ok := m.artifacts[hash+":"+project]
	return ok, nil
}
func (m *memStore) ReadArtifact(_ context.Context, hash, project string) ([]byte, error) {
	data, ok := m.artifacts[hash+":"+project]
	if !ok {
		return nil, io.ErrUnexpectedEOF
	}
	return data, nil
}
func (m *memStore) WriteArtifact(_ context.Context, _, _ string, _ []byte) error { return nil }
func (m *memStore) ReadCursor(_ context.Context) (*storage.IngestionCursor, error) {
	return nil, nil
}
func (m *memStore) WriteCursor(_ context.Context, _ *storage.IngestionCursor) error { return nil }
func (m *memStore) WriteRawArchive(_ context.Context, _ string, _ []byte) error     { return nil }

type silentLogger struct{}

func (l *silentLogger) Info(_ string) {}
func (l *silentLogger) Warn(_ string) {}

// newTestHandler creates a handler with an in-memory store and a no-op source.
func newTestHandler(store storage.ArtifactStore) *handler {
	return &handler{
		store: store,
		src:   &source.Config{Bucket: "test-bucket", Prefix: "test-prefix"},
		log:   &silentLogger{},
	}
}

func TestHandleGetHealth(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	handleGetHealth(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["status"] != "ok" {
		t.Errorf("expected status=ok, got %q", body["status"])
	}
	if body["service"] == "" {
		t.Error("expected service field to be set")
	}
}

func TestHandlePostArtifacts_MissingCommitSha(t *testing.T) {
	h := newTestHandler(&memStore{})

	body, _ := json.Marshal(map[string]any{})
	req := httptest.NewRequest(http.MethodPost, "/artifacts", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.handlePostArtifacts(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestHandlePostArtifacts_InvalidJSON(t *testing.T) {
	h := newTestHandler(&memStore{})

	req := httptest.NewRequest(http.MethodPost, "/artifacts", bytes.NewReader([]byte("not-json")))
	w := httptest.NewRecorder()

	h.handlePostArtifacts(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestHandlePostArtifacts_SelectiveRestore(t *testing.T) {
	const project1 = "packages/foo/tsconfig.json"
	const project2 = "packages/bar/tsconfig.json"
	const hash1 = "aaaa1111bbbb2222cccc3333dddd4444eeee5555ffff6666aaaa1111bbbb2222"
	const hash2 = "bbbb2222cccc3333dddd4444eeee5555ffff6666aaaa1111bbbb2222cccc3333"
	blob1 := []byte("blob-data-for-foo")
	blob2 := []byte("blob-data-for-bar")

	store := &memStore{
		index: storage.CommitIndex{
			project1: hash1,
			project2: hash2,
		},
		artifacts: map[string][]byte{
			hash1 + ":" + project1: blob1,
			hash2 + ":" + project2: blob2,
		},
	}
	h := newTestHandler(store)

	reqBody, _ := json.Marshal(map[string]any{
		"commitSha": "abc123def456",
		"projects":  []string{project1, project2},
	})
	req := httptest.NewRequest(http.MethodPost, "/artifacts", bytes.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.handlePostArtifacts(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", w.Code, w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/x-artifact-stream" {
		t.Errorf("expected Content-Type=application/x-artifact-stream, got %q", ct)
	}
	if count := w.Header().Get("X-Project-Count"); count != "2" {
		t.Errorf("expected X-Project-Count=2, got %q", count)
	}

	// Parse the length-prefixed framed response.
	data := w.Body.Bytes()
	reader := bytes.NewReader(data)
	var received []string
	for {
		var lenBuf [4]byte
		if _, err := io.ReadFull(reader, lenBuf[:]); err != nil {
			break
		}
		size := binary.BigEndian.Uint32(lenBuf[:])
		blob := make([]byte, size)
		if _, err := io.ReadFull(reader, blob); err != nil {
			t.Fatal("unexpected EOF reading blob")
		}
		received = append(received, string(blob))
	}

	if len(received) != 2 {
		t.Fatalf("expected 2 blobs, got %d", len(received))
	}
	got := map[string]bool{received[0]: true, received[1]: true}
	if !got[string(blob1)] {
		t.Errorf("blob1 %q not found in response", blob1)
	}
	if !got[string(blob2)] {
		t.Errorf("blob2 %q not found in response", blob2)
	}
}

func TestHandlePostArtifacts_SelectiveRestore_DeduplicatesHashes(t *testing.T) {
	// Two projects share the same content hash — the server must send only one blob.
	const project1 = "packages/foo/tsconfig.json"
	const project2 = "packages/foo-alias/tsconfig.json"
	const sharedHash = "aaaa1111bbbb2222cccc3333dddd4444eeee5555ffff6666aaaa1111bbbb2222"
	blob := []byte("shared-blob")

	store := &memStore{
		index: storage.CommitIndex{
			project1: sharedHash,
			project2: sharedHash,
		},
		artifacts: map[string][]byte{
			sharedHash + ":" + project1: blob,
			sharedHash + ":" + project2: blob,
		},
	}
	h := newTestHandler(store)

	reqBody, _ := json.Marshal(map[string]any{
		"commitSha": "abc123def456",
		"projects":  []string{project1, project2},
	})
	req := httptest.NewRequest(http.MethodPost, "/artifacts", bytes.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.handlePostArtifacts(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if count := w.Header().Get("X-Project-Count"); count != "1" {
		t.Errorf("expected X-Project-Count=1 (deduped), got %q", count)
	}
}

func TestHandlePostArtifacts_NoIndex_TriesGCS(t *testing.T) {
	// When no index is found the handler calls GCS. With a non-existent bucket
	// the HTTP request will fail and the handler should return non-200.
	store := &memStore{index: nil}
	h := newTestHandler(store)

	reqBody, _ := json.Marshal(map[string]any{"commitSha": "abc123def456"})
	req := httptest.NewRequest(http.MethodPost, "/artifacts", bytes.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.handlePostArtifacts(w, req)

	// With no real GCS the handler should respond with 4xx/5xx, not 200.
	if w.Code == http.StatusOK {
		t.Error("expected non-200 when GCS is unreachable and no index exists")
	}
}
