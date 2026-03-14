// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.

package storage

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
)

const (
	indexPrefix    = "index/"
	artifactsPrefix = "artifacts/"
	cursorKey      = "meta/ingestion_cursor.json"
	rawPrefix      = "raw/"
	artifactExt    = ".tar.gz"
)

type localStore struct {
	baseDir string
}

// NewLocalStore returns an ArtifactStore backed by the local filesystem at baseDir.
func NewLocalStore(baseDir string) ArtifactStore {
	return &localStore{baseDir: baseDir}
}

func (s *localStore) indexPath(commitSha string) string {
	return filepath.Join(s.baseDir, indexPrefix, commitSha+".json")
}

// artifactPath returns the local filesystem path for a project artifact blob.
//
// Layout: artifacts/{projectDir}/{hash16}.tar.gz
//   - projectDir: projectPath with the trailing "/tsconfig.json" stripped
//     (e.g. "packages/@kbn/core/tsconfig.json" → "packages/@kbn/core")
//   - hash16: first 16 hex characters of the SHA-256 content hash
//     (sufficient entropy for deduplication within a project directory across commits)
//
// This structure makes the on-disk store navigable by project path (readable in
// Finder/Explorer) while preserving content-addressed deduplication.
func (s *localStore) artifactPath(contentHash, projectPath string) string {
	dir := strings.TrimSuffix(filepath.ToSlash(projectPath), "/tsconfig.json")
	return filepath.Join(s.baseDir, artifactsPrefix, filepath.FromSlash(dir), contentHash[:16]+artifactExt)
}

func (s *localStore) rawPath(commitSha string) string {
	return filepath.Join(s.baseDir, rawPrefix, commitSha+artifactExt)
}

func (s *localStore) cursorFilePath() string {
	return filepath.Join(s.baseDir, cursorKey)
}

func ensureDir(path string) error {
	return os.MkdirAll(filepath.Dir(path), 0o755)
}

func (s *localStore) ReadIndex(_ context.Context, commitSha string) (CommitIndex, error) {
	data, err := os.ReadFile(s.indexPath(commitSha))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var idx CommitIndex
	if err := json.Unmarshal(data, &idx); err != nil {
		return nil, err
	}
	return idx, nil
}

func (s *localStore) WriteIndex(_ context.Context, commitSha string, index CommitIndex) error {
	path := s.indexPath(commitSha)
	if err := ensureDir(path); err != nil {
		return err
	}
	data, err := json.Marshal(index)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func (s *localStore) HasArtifact(_ context.Context, contentHash, projectPath string) (bool, error) {
	_, err := os.Stat(s.artifactPath(contentHash, projectPath))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

func (s *localStore) ReadArtifact(_ context.Context, contentHash, projectPath string) ([]byte, error) {
	return os.ReadFile(s.artifactPath(contentHash, projectPath))
}

func (s *localStore) WriteArtifact(_ context.Context, contentHash, projectPath string, data []byte) error {
	path := s.artifactPath(contentHash, projectPath)
	if err := ensureDir(path); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func (s *localStore) ReadCursor(_ context.Context) (*IngestionCursor, error) {
	data, err := os.ReadFile(s.cursorFilePath())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var cursor IngestionCursor
	if err := json.Unmarshal(data, &cursor); err != nil {
		return nil, err
	}
	return &cursor, nil
}

func (s *localStore) WriteCursor(_ context.Context, cursor *IngestionCursor) error {
	path := s.cursorFilePath()
	if err := ensureDir(path); err != nil {
		return err
	}
	data, err := json.Marshal(cursor)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func (s *localStore) WriteRawArchive(_ context.Context, commitSha string, data []byte) error {
	path := s.rawPath(commitSha)
	if err := ensureDir(path); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}
