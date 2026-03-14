// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.

package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestLocalStore_ArtifactRoundtrip(t *testing.T) {
	dir := t.TempDir()
	store := NewLocalStore(dir)
	ctx := context.Background()

	project := "packages/foo/tsconfig.json"
	hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	data := []byte("fake-gzipped-tar-content")

	// Initially absent.
	ok, err := store.HasArtifact(ctx, hash, project)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Error("expected HasArtifact=false before writing")
	}

	// Write.
	if err := store.WriteArtifact(ctx, hash, project, data); err != nil {
		t.Fatal(err)
	}

	// Now present.
	ok, err = store.HasArtifact(ctx, hash, project)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("expected HasArtifact=true after writing")
	}

	// Read back matches original.
	got, err := store.ReadArtifact(ctx, hash, project)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Errorf("ReadArtifact = %q, want %q", got, data)
	}
}

func TestLocalStore_ArtifactPathLayout(t *testing.T) {
	dir := t.TempDir()
	store := NewLocalStore(dir)
	ctx := context.Background()

	// Verify the on-disk path is: artifacts/{projectDir}/{hash16}.tar.gz
	project := "packages/@kbn/core/tsconfig.json"
	hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	if err := store.WriteArtifact(ctx, hash, project, []byte("data")); err != nil {
		t.Fatal(err)
	}

	expected := filepath.Join(dir, "artifacts", "packages/@kbn/core", hash[:16]+".tar.gz")
	if _, err := os.Stat(expected); err != nil {
		t.Errorf("artifact not found at expected path %s: %v", expected, err)
	}
}

func TestLocalStore_ArtifactDeduplication(t *testing.T) {
	dir := t.TempDir()
	store := NewLocalStore(dir)
	ctx := context.Background()

	// Two projects with the same content hash share the same file on disk
	// (idempotent write — second write is a no-op since HasArtifact is checked
	// by the caller, but WriteArtifact itself must not error on overwrite).
	hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	data := []byte("shared-blob")

	if err := store.WriteArtifact(ctx, hash, "packages/foo/tsconfig.json", data); err != nil {
		t.Fatal(err)
	}
	// Writing a second time (same hash, same project) must not error.
	if err := store.WriteArtifact(ctx, hash, "packages/foo/tsconfig.json", data); err != nil {
		t.Errorf("second WriteArtifact should not error: %v", err)
	}
}

func TestLocalStore_IndexRoundtrip(t *testing.T) {
	dir := t.TempDir()
	store := NewLocalStore(dir)
	ctx := context.Background()

	// Missing index returns nil.
	idx, err := store.ReadIndex(ctx, "sha_missing")
	if err != nil {
		t.Fatal(err)
	}
	if idx != nil {
		t.Errorf("expected nil for missing index, got %v", idx)
	}

	// Write and read back.
	index := CommitIndex{
		"packages/foo/tsconfig.json": "hash1",
		"packages/bar/tsconfig.json": "hash2",
	}
	if err := store.WriteIndex(ctx, "abc123", index); err != nil {
		t.Fatal(err)
	}
	got, err := store.ReadIndex(ctx, "abc123")
	if err != nil {
		t.Fatal(err)
	}
	if got["packages/foo/tsconfig.json"] != "hash1" || got["packages/bar/tsconfig.json"] != "hash2" {
		t.Errorf("index mismatch: got %v", got)
	}
}

func TestLocalStore_CursorRoundtrip(t *testing.T) {
	dir := t.TempDir()
	store := NewLocalStore(dir)
	ctx := context.Background()

	// Missing cursor returns nil.
	c, err := store.ReadCursor(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if c != nil {
		t.Errorf("expected nil cursor, got %v", c)
	}

	cursor := &IngestionCursor{
		ProcessedCommits: []string{"sha1", "sha2"},
		LastPollAt:       "2026-01-01T00:00:00Z",
	}
	if err := store.WriteCursor(ctx, cursor); err != nil {
		t.Fatal(err)
	}
	got, err := store.ReadCursor(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.ProcessedCommits) != 2 || got.ProcessedCommits[0] != "sha1" || got.LastPollAt != "2026-01-01T00:00:00Z" {
		t.Errorf("cursor mismatch: got %+v", got)
	}
}
