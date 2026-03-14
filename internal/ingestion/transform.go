// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.

// Package ingestion handles the extraction and transformation of monolithic CI archives
// into granular, content-addressed per-project artifact blobs.
package ingestion

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"kbn-ts-type-check-oblt-server-go/internal/storage"
)

const tmpPrefix = "oblt-cache-ingest-"

// Logger is the minimal logging interface used by ingestion functions.
type Logger interface {
	Info(msg string)
	Warn(msg string)
}

type fileEntry struct {
	relativePath string
	absolutePath string
}

// toProjectPath maps an archive entry path to its owning project's tsconfig.json path.
//
// Archive paths follow two conventions:
//   - Build outputs:  packages/foo/target/types/...  → packages/foo/tsconfig.json
//   - Config/binfo:   packages/foo/tsconfig*.json    → packages/foo/tsconfig.json
//   - Build info:     packages/foo/*.tsbuildinfo     → packages/foo/tsconfig.json
func toProjectPath(entryPath string) string {
	normalized := filepath.ToSlash(entryPath)

	if idx := strings.Index(normalized, "/target/"); idx != -1 {
		return normalized[:idx] + "/tsconfig.json"
	}

	dir := filepath.ToSlash(filepath.Dir(normalized))
	if dir == "." || dir == "" {
		return "tsconfig.json"
	}
	return dir + "/tsconfig.json"
}

// extractToTemp writes archiveData (a .tar.gz) into tmpDir and returns a flat list of
// all extracted file entries with their relative and absolute paths.
func extractToTemp(archiveData []byte, tmpDir string) ([]fileEntry, error) {
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return nil, err
	}

	gr, err := gzip.NewReader(bytes.NewReader(archiveData))
	if err != nil {
		return nil, fmt.Errorf("open gzip reader: %w", err)
	}
	defer gr.Close()

	tr := tar.NewReader(gr)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read tar entry: %w", err)
		}

		// Sanitize path to prevent directory traversal.
		cleanName := filepath.Clean(hdr.Name)
		if strings.HasPrefix(cleanName, "..") {
			continue
		}
		dest := filepath.Join(tmpDir, cleanName)

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(dest, 0o755); err != nil {
				return nil, err
			}
		case tar.TypeReg, tar.TypeRegA:
			if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
				return nil, err
			}
			f, err := os.Create(dest)
			if err != nil {
				return nil, err
			}
			_, copyErr := io.Copy(f, tr)
			f.Close()
			if copyErr != nil {
				return nil, fmt.Errorf("write %s: %w", hdr.Name, copyErr)
			}
		}
	}

	return walkDir(tmpDir, tmpDir, "")
}

func walkDir(baseDir, dir, base string) ([]fileEntry, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var result []fileEntry
	for _, e := range entries {
		name := e.Name()
		// Skip the archive file itself if it was written to tmpDir.
		if name == "archive.tar.gz" || name == "archive.tar" {
			continue
		}

		rel := name
		if base != "" {
			rel = base + "/" + name
		}
		abs := filepath.Join(dir, name)

		if e.IsDir() {
			sub, err := walkDir(baseDir, abs, rel)
			if err != nil {
				return nil, err
			}
			result = append(result, sub...)
		} else {
			result = append(result, fileEntry{relativePath: rel, absolutePath: abs})
		}
	}
	return result, nil
}

// groupByProject groups extracted file entries by the project path they belong to.
func groupByProject(entries []fileEntry) map[string][]fileEntry {
	byProject := make(map[string][]fileEntry)
	for _, e := range entries {
		key := toProjectPath(e.relativePath)
		byProject[key] = append(byProject[key], e)
	}
	return byProject
}

// createProjectTar builds a raw (uncompressed) tar archive containing the files for one
// project. Paths inside the archive are relative to cwd.
func createProjectTar(files []fileEntry, cwd string) ([]byte, error) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	for _, f := range files {
		info, err := os.Stat(f.absolutePath)
		if err != nil {
			return nil, err
		}

		relPath, err := filepath.Rel(cwd, f.absolutePath)
		if err != nil {
			return nil, err
		}
		relPath = filepath.ToSlash(relPath)

		hdr, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return nil, err
		}
		hdr.Name = relPath
		// Normalize all filesystem metadata so the content hash reflects only
		// file content and path, not extraction-time mtimes, platform uid/gid,
		// or umask-dependent mode bits.
		// Without this, every ingestion run produces unique blobs for unchanged
		// projects (mtime = "now" on extraction), defeating deduplication.
		hdr.ModTime = time.Time{}
		hdr.AccessTime = time.Time{}
		hdr.ChangeTime = time.Time{}
		hdr.Uid, hdr.Gid = 0, 0
		hdr.Uname, hdr.Gname = "", ""
		// Mode bits from os.Create depend on the process umask (e.g. 0644 vs 0664),
		// which varies by host. Normalize to fixed values so the hash is stable.
		if info.IsDir() {
			hdr.Mode = 0o755
		} else {
			hdr.Mode = 0o644
		}

		if err := tw.WriteHeader(hdr); err != nil {
			return nil, err
		}

		file, err := os.Open(f.absolutePath)
		if err != nil {
			return nil, err
		}
		_, copyErr := io.Copy(tw, file)
		file.Close()
		if copyErr != nil {
			return nil, fmt.Errorf("copy %s into tar: %w", relPath, copyErr)
		}
	}

	if err := tw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// gzipBytes compresses data with gzip at best compression.
func gzipBytes(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	gw, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	if err != nil {
		return nil, err
	}
	if _, err := gw.Write(data); err != nil {
		return nil, err
	}
	if err := gw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func sha256Hex(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

// shortSha returns up to the first 12 characters of a commit SHA for use in
// log messages and temp-dir names where a full SHA is not necessary.
func shortSha(sha string) string {
	if len(sha) <= 12 {
		return sha
	}
	return sha[:12]
}

// TransformAndStore ingests one monolithic archive:
//  1. Extracts the .tar.gz into a temporary directory.
//  2. Groups files by project path.
//  3. For each project, creates a gzipped tar blob, computes its SHA-256, and writes it to store.
//  4. Returns the CommitIndex (project path → content hash) for the caller to persist.
func TransformAndStore(ctx context.Context, archiveData []byte, commitSha string, store storage.ArtifactStore, log Logger) (storage.CommitIndex, error) {
	tmpDir, err := os.MkdirTemp("", tmpPrefix+shortSha(commitSha))
	if err != nil {
		return nil, fmt.Errorf("create temp dir: %w", err)
	}
	defer func() {
		if removeErr := os.RemoveAll(tmpDir); removeErr != nil {
			log.Warn(fmt.Sprintf("cleanup temp dir %s: %v", tmpDir, removeErr))
		}
	}()

	entries, err := extractToTemp(archiveData, tmpDir)
	if err != nil {
		return nil, fmt.Errorf("extract archive: %w", err)
	}
	if len(entries) == 0 {
		log.Warn(fmt.Sprintf("no files found in archive for %s", commitSha))
		return storage.CommitIndex{}, nil
	}

	byProject := groupByProject(entries)
	index := make(storage.CommitIndex, len(byProject))

	for projectPath, files := range byProject {
		tarBuf, err := createProjectTar(files, tmpDir)
		if err != nil {
			return nil, fmt.Errorf("create tar for %s: %w", projectPath, err)
		}

		gzipped, err := gzipBytes(tarBuf)
		if err != nil {
			return nil, fmt.Errorf("gzip tar for %s: %w", projectPath, err)
		}

		contentHash := sha256Hex(gzipped)

		exists, err := store.HasArtifact(ctx, contentHash, projectPath)
		if err != nil {
			return nil, fmt.Errorf("check artifact %s: %w", contentHash[:12], err)
		}
		if !exists {
			if err := store.WriteArtifact(ctx, contentHash, projectPath, gzipped); err != nil {
				return nil, fmt.Errorf("write artifact %s: %w", contentHash[:12], err)
			}
		}

		index[projectPath] = contentHash
	}

	log.Info(fmt.Sprintf("ingested %s: %d projects, %d index entries",
		shortSha(commitSha), len(byProject), len(index)))

	return index, nil
}
