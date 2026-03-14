// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.

// Package source provides read-only access to the GCS bucket that holds monolithic
// CI-produced TypeScript type-check archives. It uses the public GCS JSON/Storage APIs
// so no authentication is required for public buckets.
package source

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

const (
	gcsListAPI        = "https://storage.googleapis.com/storage/v1/b"
	gcsStorageBase    = "https://storage.googleapis.com"
	commitsPath       = "commits"
	archiveFilename   = "archive.tar.gz"
)

// Config identifies the source GCS bucket and object prefix.
type Config struct {
	Bucket string
	Prefix string
}

// archiveURL returns the public HTTPS URL for a commit's monolithic archive.
func (c *Config) archiveURL(commitSha string) string {
	path := fmt.Sprintf("%s/%s/%s/%s", c.Prefix, commitsPath, commitSha, archiveFilename)
	return fmt.Sprintf("%s/%s/%s", gcsStorageBase, c.Bucket, path)
}

// CommitEntry pairs a commit SHA with the time its archive was uploaded to GCS.
type CommitEntry struct {
	SHA       string
	CreatedAt time.Time
}

// ListCommits returns all commits that have an archive in the source bucket,
// sorted newest-first by GCS upload time. This ensures the poll cycle ingests
// the most recent commits first so fresh data is available to CLI users as
// quickly as possible.
//
// Uses items(name,timeCreated) instead of the delimiter-based prefix listing
// because virtual-directory prefixes carry no timestamp metadata.
func ListCommits(cfg *Config) ([]CommitEntry, error) {
	objectPrefix := cfg.Prefix + "/" + commitsPath + "/"
	archiveSuffix := "/" + archiveFilename
	baseURL := fmt.Sprintf("%s/%s/o", gcsListAPI, cfg.Bucket)
	var commits []CommitEntry

	pageToken := ""
	for {
		params := url.Values{
			"prefix":     {objectPrefix},
			"maxResults": {"1000"},
			"fields":     {"items(name,timeCreated),nextPageToken"},
		}
		if pageToken != "" {
			params.Set("pageToken", pageToken)
		}

		resp, err := http.Get(baseURL + "?" + params.Encode())
		if err != nil {
			return nil, fmt.Errorf("GCS list request: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, fmt.Errorf("GCS list API returned %d", resp.StatusCode)
		}

		var result struct {
			Items []struct {
				Name        string `json:"name"`
				TimeCreated string `json:"timeCreated"`
			} `json:"items"`
			NextPageToken string `json:"nextPageToken"`
		}
		decodeErr := json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()
		if decodeErr != nil {
			return nil, fmt.Errorf("decode GCS list response: %w", decodeErr)
		}

		for _, item := range result.Items {
			if !strings.HasSuffix(item.Name, archiveSuffix) {
				continue
			}
			// Extract SHA from "{prefix}/commits/{sha}/archive.tar.gz"
			inner := strings.TrimPrefix(item.Name, objectPrefix)
			sha := strings.TrimSuffix(inner, archiveSuffix)
			if sha == "" || strings.Contains(sha, "/") {
				continue
			}
			t, parseErr := time.Parse(time.RFC3339, item.TimeCreated)
			if parseErr != nil {
				t = time.Time{}
			}
			commits = append(commits, CommitEntry{SHA: sha, CreatedAt: t})
		}

		pageToken = result.NextPageToken
		if pageToken == "" {
			break
		}
	}

	// Newest first: most recently uploaded archive is ingested before older history.
	sort.Slice(commits, func(i, j int) bool {
		return commits[i].CreatedAt.After(commits[j].CreatedAt)
	})

	return commits, nil
}

// DownloadArchive fetches the full monolithic archive for a commit and returns it as bytes.
func DownloadArchive(cfg *Config, commitSha string) ([]byte, error) {
	resp, err := http.Get(cfg.archiveURL(commitSha))
	if err != nil {
		return nil, fmt.Errorf("download archive for %s: %w", commitSha, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("download archive for %s: HTTP %d", commitSha, resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read archive body for %s: %w", commitSha, err)
	}
	return data, nil
}

// ArchiveStream holds a streaming HTTP response for a GCS archive.
type ArchiveStream struct {
	// Body is the raw response body; the caller must Close it.
	Body io.ReadCloser
	// ContentLength is the value from the Content-Length header, or -1 if absent.
	ContentLength int64
}

// DownloadArchiveStream opens a streaming download of the monolithic archive without
// buffering the full body. The caller is responsible for closing ArchiveStream.Body.
func DownloadArchiveStream(cfg *Config, commitSha string) (*ArchiveStream, error) {
	resp, err := http.Get(cfg.archiveURL(commitSha))
	if err != nil {
		return nil, fmt.Errorf("stream archive for %s: %w", commitSha, err)
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("stream archive for %s: HTTP %d", commitSha, resp.StatusCode)
	}

	cl := resp.ContentLength // -1 if not provided by the server
	return &ArchiveStream{Body: resp.Body, ContentLength: cl}, nil
}
