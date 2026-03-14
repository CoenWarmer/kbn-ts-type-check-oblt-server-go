// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.

// Package artifacts assembles per-project artifact blobs into a streaming tar.gz.
package artifacts

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"sync"

	"kbn-ts-type-check-oblt-server-go/internal/storage"
)

const (
	tarBlock  = 512
	batchSize = 16
)

// stripEndOfArchive removes trailing all-zero 512-byte blocks from a raw tar buffer.
// Each per-project tar.Pack ends with two zero blocks (end-of-archive markers). When
// multiple project tars are concatenated, these intermediate markers cause tar readers
// to stop reading at the first end-of-archive. Stripping them keeps the output as a
// single continuous archive terminated only by the enclosing gzip stream's EOF.
func stripEndOfArchive(raw []byte) []byte {
	end := len(raw)
	for end >= tarBlock {
		allZero := true
		for _, b := range raw[end-tarBlock : end] {
			if b != 0 {
				allZero = false
				break
			}
		}
		if !allZero {
			break
		}
		end -= tarBlock
	}
	if end < len(raw) {
		return raw[:end]
	}
	return raw
}

// gunzipBytes decompresses a gzip-encoded byte slice.
func gunzipBytes(data []byte) ([]byte, error) {
	gr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer gr.Close()
	return io.ReadAll(gr)
}

type blobResult struct {
	data []byte
	err  error
}

// projectBlob pairs a project path with its content hash for store lookups.
type projectBlob struct {
	project string
	hash    string
}

// CreateArtifactsStream assembles the requested project artifacts into a streaming tar.gz.
//
// Algorithm:
//  1. Deduplicate content hashes, preserving insertion order.
//  2. Open an io.Pipe; the read end is returned immediately to the caller (HTTP handler).
//  3. A goroutine launches one goroutine per unique hash (capped at batchSize concurrent
//     reads) to read and gunzip each blob from the store in parallel.
//  4. Results are collected in original order and written sequentially into a gzip.Writer
//     that targets the pipe's write end.
//  5. End-of-archive markers are stripped from each raw tar before writing so the output
//     is a single valid tar stream terminated by the enclosing gzip EOF.
//
// The pipe provides natural backpressure: the store reads pause automatically when the
// HTTP client reads too slowly, keeping memory bounded to O(batchSize) decompressed blobs.
func CreateArtifactsStream(ctx context.Context, projectToHash map[string]string, store storage.ArtifactStore) io.Reader {
	// Deduplicate hashes while preserving the order they were first seen.
	// Both project path and hash are kept so ReadArtifact can locate the blob
	// in the project-scoped directory layout.
	seen := make(map[string]struct{})
	var ordered []projectBlob
	for project, hash := range projectToHash {
		if _, ok := seen[hash]; !ok {
			seen[hash] = struct{}{}
			ordered = append(ordered, projectBlob{project: project, hash: hash})
		}
	}

	pr, pw := io.Pipe()

	go func() {
		if err := writeAllArtifacts(ctx, ordered, store, pw); err != nil {
			pw.CloseWithError(err)
		} else {
			pw.Close()
		}
	}()

	return pr
}

func writeAllArtifacts(ctx context.Context, blobs []projectBlob, store storage.ArtifactStore, w io.WriteCloser) error {
	gz, err := gzip.NewWriterLevel(w, gzip.BestSpeed)
	if err != nil {
		return fmt.Errorf("create gzip writer: %w", err)
	}
	defer gz.Close()

	// Process blobs in batches: launch concurrent reads, write results in order.
	for i := 0; i < len(blobs); i += batchSize {
		end := i + batchSize
		if end > len(blobs) {
			end = len(blobs)
		}
		batch := blobs[i:end]

		results := make([]chan blobResult, len(batch))
		var wg sync.WaitGroup

		for j, item := range batch {
			results[j] = make(chan blobResult, 1)
			wg.Add(1)
			go func(ch chan<- blobResult, pb projectBlob) {
				defer wg.Done()
				gzipped, err := store.ReadArtifact(ctx, pb.hash, pb.project)
				if err != nil {
					ch <- blobResult{err: fmt.Errorf("read artifact %s: %w", pb.hash[:12], err)}
					return
				}
				raw, err := gunzipBytes(gzipped)
				if err != nil {
					ch <- blobResult{err: fmt.Errorf("decompress artifact %s: %w", pb.hash[:12], err)}
					return
				}
				ch <- blobResult{data: stripEndOfArchive(raw)}
			}(results[j], item)
		}

		// Wait for all goroutines in this batch to finish before consuming results,
		// ensuring we can safely close the channels before ranging over them.
		wg.Wait()

		for _, ch := range results {
			r := <-ch
			if r.err != nil {
				return r.err
			}
			if _, err := gz.Write(r.data); err != nil {
				return fmt.Errorf("write artifact to gzip stream: %w", err)
			}
		}
	}

	return nil
}
