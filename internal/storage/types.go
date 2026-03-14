// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.

package storage

import "context"

// CommitIndex maps project path (e.g. "packages/foo/tsconfig.json") to a content hash.
type CommitIndex map[string]string

// IngestionCursor tracks which commit SHAs have been fully ingested.
type IngestionCursor struct {
	ProcessedCommits []string `json:"processedCommits"`
	LastPollAt       string   `json:"lastPollAt,omitempty"`
}

// ArtifactStore abstracts the destination for ingested artifacts (local FS or GCS).
type ArtifactStore interface {
	// ReadIndex returns the CommitIndex for a commit, or nil if not yet ingested.
	ReadIndex(ctx context.Context, commitSha string) (CommitIndex, error)

	// WriteIndex persists the CommitIndex for a commit after all artifacts are written.
	WriteIndex(ctx context.Context, commitSha string, index CommitIndex) error

	// HasArtifact reports whether the gzipped-tar blob for a content hash exists.
	// projectPath (e.g. "packages/foo/tsconfig.json") scopes the storage location
	// so the on-disk layout mirrors the project tree for human readability.
	HasArtifact(ctx context.Context, contentHash string, projectPath string) (bool, error)

	// ReadArtifact returns the gzipped-tar blob for a content hash.
	ReadArtifact(ctx context.Context, contentHash string, projectPath string) ([]byte, error)

	// WriteArtifact stores the gzipped-tar blob. Callers check HasArtifact first for
	// idempotency; implementations may also deduplicate internally.
	WriteArtifact(ctx context.Context, contentHash string, projectPath string, data []byte) error

	// ReadCursor returns the ingestion cursor, or nil if none has been written yet.
	ReadCursor(ctx context.Context) (*IngestionCursor, error)

	// WriteCursor persists the ingestion cursor.
	WriteCursor(ctx context.Context, cursor *IngestionCursor) error

	// WriteRawArchive stores a copy of the raw monolithic archive for a commit.
	WriteRawArchive(ctx context.Context, commitSha string, data []byte) error
}
