// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.

package ingestion

import (
	"bytes"
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"testing"
	"time"

	"kbn-ts-type-check-oblt-server-go/internal/source"
	"kbn-ts-type-check-oblt-server-go/internal/storage"
)

// emptyArchive returns a valid but empty .tar.gz so TransformAndStore
// does not error on extraction (it will warn "no files found" and return {}).
func emptyArchive() []byte {
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gw)
	_ = tw.Close()
	_ = gw.Close()
	return buf.Bytes()
}

type silentLog struct{}

func (l *silentLog) Info(_ string) {}
func (l *silentLog) Warn(_ string) {}

func TestRunPollCycle_NoNewCommits(t *testing.T) {
	dir := t.TempDir()
	store := storage.NewLocalStore(dir)
	ctx := context.Background()

	// Pre-mark sha1 as processed.
	_ = store.WriteCursor(ctx, &storage.IngestionCursor{ProcessedCommits: []string{"sha1"}})

	downloaded := 0
	fns := PollFuncs{
		ListCommits: func(_ *source.Config) ([]source.CommitEntry, error) {
			return []source.CommitEntry{{SHA: "sha1", CreatedAt: time.Now()}}, nil
		},
		DownloadArchive: func(_ *source.Config, _ string) ([]byte, error) {
			downloaded++
			return emptyArchive(), nil
		},
	}

	if err := RunPollCycle(ctx, &source.Config{}, store, &silentLog{}, fns); err != nil {
		t.Fatal(err)
	}
	if downloaded != 0 {
		t.Errorf("expected 0 downloads when all commits are processed, got %d", downloaded)
	}
}

func TestRunPollCycle_SkipsAlreadyProcessedCommits(t *testing.T) {
	dir := t.TempDir()
	store := storage.NewLocalStore(dir)
	ctx := context.Background()

	_ = store.WriteCursor(ctx, &storage.IngestionCursor{ProcessedCommits: []string{"sha1"}})

	var ingested []string
	fns := PollFuncs{
		ListCommits: func(_ *source.Config) ([]source.CommitEntry, error) {
			return []source.CommitEntry{
				{SHA: "sha2", CreatedAt: time.Now()},
				{SHA: "sha1", CreatedAt: time.Now().Add(-time.Hour)},
			}, nil
		},
		DownloadArchive: func(_ *source.Config, sha string) ([]byte, error) {
			ingested = append(ingested, sha)
			return emptyArchive(), nil
		},
	}

	if err := RunPollCycle(ctx, &source.Config{}, store, &silentLog{}, fns); err != nil {
		t.Fatal(err)
	}
	if len(ingested) != 1 || ingested[0] != "sha2" {
		t.Errorf("expected only sha2 to be ingested, got %v", ingested)
	}
}

func TestRunPollCycle_UpdatesCursorAfterEachCommit(t *testing.T) {
	dir := t.TempDir()
	store := storage.NewLocalStore(dir)
	ctx := context.Background()

	fns := PollFuncs{
		ListCommits: func(_ *source.Config) ([]source.CommitEntry, error) {
			return []source.CommitEntry{
				{SHA: "sha1", CreatedAt: time.Now()},
				{SHA: "sha2", CreatedAt: time.Now().Add(-time.Hour)},
			}, nil
		},
		DownloadArchive: func(_ *source.Config, _ string) ([]byte, error) {
			return emptyArchive(), nil
		},
	}

	if err := RunPollCycle(ctx, &source.Config{}, store, &silentLog{}, fns); err != nil {
		t.Fatal(err)
	}

	cursor, err := store.ReadCursor(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cursor == nil {
		t.Fatal("expected cursor to be written")
	}

	processed := make(map[string]bool)
	for _, sha := range cursor.ProcessedCommits {
		processed[sha] = true
	}
	if !processed["sha1"] || !processed["sha2"] {
		t.Errorf("expected both sha1 and sha2 in cursor, got %v", cursor.ProcessedCommits)
	}
}

func TestRunPollCycle_ContinuesAfterDownloadError(t *testing.T) {
	dir := t.TempDir()
	store := storage.NewLocalStore(dir)
	ctx := context.Background()

	var ingested []string
	fns := PollFuncs{
		ListCommits: func(_ *source.Config) ([]source.CommitEntry, error) {
			return []source.CommitEntry{
				{SHA: "sha1", CreatedAt: time.Now()},
				{SHA: "sha2", CreatedAt: time.Now().Add(-time.Hour)},
			}, nil
		},
		DownloadArchive: func(_ *source.Config, sha string) ([]byte, error) {
			if sha == "sha1" {
				return nil, errors.New("network error")
			}
			ingested = append(ingested, sha)
			return emptyArchive(), nil
		},
	}

	// The cycle should not return an error — it logs the failure and continues.
	if err := RunPollCycle(ctx, &source.Config{}, store, &silentLog{}, fns); err != nil {
		t.Fatal(err)
	}
	// sha1 failed, sha2 should still be ingested.
	if len(ingested) != 1 || ingested[0] != "sha2" {
		t.Errorf("expected sha2 to be ingested after sha1 failure, got %v", ingested)
	}
}

func TestRunPollCycle_NewestFirst(t *testing.T) {
	dir := t.TempDir()
	store := storage.NewLocalStore(dir)
	ctx := context.Background()

	var order []string
	now := time.Now()
	fns := PollFuncs{
		ListCommits: func(_ *source.Config) ([]source.CommitEntry, error) {
			// Return newest-first (as ListCommits guarantees).
			return []source.CommitEntry{
				{SHA: "new", CreatedAt: now},
				{SHA: "old", CreatedAt: now.Add(-24 * time.Hour)},
			}, nil
		},
		DownloadArchive: func(_ *source.Config, sha string) ([]byte, error) {
			order = append(order, sha)
			return emptyArchive(), nil
		},
	}

	if err := RunPollCycle(ctx, &source.Config{}, store, &silentLog{}, fns); err != nil {
		t.Fatal(err)
	}
	if len(order) != 2 || order[0] != "new" || order[1] != "old" {
		t.Errorf("expected newest-first order [new old], got %v", order)
	}
}
