// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.

package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	gcs "cloud.google.com/go/storage"
)

type gcsStore struct {
	bucket *gcs.BucketHandle
}

// NewGCSStore returns an ArtifactStore backed by a GCS bucket.
// Authentication uses Application Default Credentials.
func NewGCSStore(ctx context.Context, bucketName string) (ArtifactStore, error) {
	client, err := gcs.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("create GCS client: %w", err)
	}
	return &gcsStore{bucket: client.Bucket(bucketName)}, nil
}

func isNotFound(err error) bool {
	return errors.Is(err, gcs.ErrObjectNotExist)
}

func (s *gcsStore) readObject(ctx context.Context, key string) ([]byte, error) {
	rc, err := s.bucket.Object(key).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

func (s *gcsStore) writeObject(ctx context.Context, key string, data []byte, contentType string) error {
	wc := s.bucket.Object(key).NewWriter(ctx)
	wc.ContentType = contentType
	if _, err := io.Copy(wc, bytes.NewReader(data)); err != nil {
		wc.Close()
		return err
	}
	return wc.Close()
}

func gcsIndexKey(commitSha string) string {
	return indexPrefix + commitSha + ".json"
}

// gcsArtifactKey returns the GCS object key for a project artifact blob.
// Mirrors the local store layout: artifacts/{projectDir}/{hash16}.tar.gz
func gcsArtifactKey(contentHash, projectPath string) string {
	dir := strings.TrimSuffix(projectPath, "/tsconfig.json")
	return artifactsPrefix + dir + "/" + contentHash[:16] + artifactExt
}

func gcsRawKey(commitSha string) string {
	return rawPrefix + commitSha + artifactExt
}

func (s *gcsStore) ReadIndex(ctx context.Context, commitSha string) (CommitIndex, error) {
	data, err := s.readObject(ctx, gcsIndexKey(commitSha))
	if isNotFound(err) {
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

func (s *gcsStore) WriteIndex(ctx context.Context, commitSha string, index CommitIndex) error {
	data, err := json.Marshal(index)
	if err != nil {
		return err
	}
	return s.writeObject(ctx, gcsIndexKey(commitSha), data, "application/json")
}

func (s *gcsStore) HasArtifact(ctx context.Context, contentHash, projectPath string) (bool, error) {
	_, err := s.bucket.Object(gcsArtifactKey(contentHash, projectPath)).Attrs(ctx)
	if isNotFound(err) {
		return false, nil
	}
	return err == nil, err
}

func (s *gcsStore) ReadArtifact(ctx context.Context, contentHash, projectPath string) ([]byte, error) {
	return s.readObject(ctx, gcsArtifactKey(contentHash, projectPath))
}

func (s *gcsStore) WriteArtifact(ctx context.Context, contentHash, projectPath string, data []byte) error {
	exists, err := s.HasArtifact(ctx, contentHash, projectPath)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	return s.writeObject(ctx, gcsArtifactKey(contentHash, projectPath), data, "application/gzip")
}

func (s *gcsStore) ReadCursor(ctx context.Context) (*IngestionCursor, error) {
	data, err := s.readObject(ctx, cursorKey)
	if isNotFound(err) {
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

func (s *gcsStore) WriteCursor(ctx context.Context, cursor *IngestionCursor) error {
	data, err := json.Marshal(cursor)
	if err != nil {
		return err
	}
	return s.writeObject(ctx, cursorKey, data, "application/json")
}

func (s *gcsStore) WriteRawArchive(ctx context.Context, commitSha string, data []byte) error {
	return s.writeObject(ctx, gcsRawKey(commitSha), data, "application/gzip")
}
