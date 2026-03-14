// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.

package ingestion

import (
	"context"
	"fmt"
	"sort"
	"time"

	"kbn-ts-type-check-oblt-server-go/internal/source"
	"kbn-ts-type-check-oblt-server-go/internal/storage"
)

// PollFuncs holds the GCS-facing functions used by RunPollCycle.
// The zero value falls back to the real production implementations.
// Override individual fields in tests to avoid real network calls.
type PollFuncs struct {
	ListCommits     func(cfg *source.Config) ([]source.CommitEntry, error)
	DownloadArchive func(cfg *source.Config, sha string) ([]byte, error)
}

func (f *PollFuncs) listCommits(cfg *source.Config) ([]source.CommitEntry, error) {
	if f.ListCommits != nil {
		return f.ListCommits(cfg)
	}
	return source.ListCommits(cfg)
}

func (f *PollFuncs) downloadArchive(cfg *source.Config, sha string) ([]byte, error) {
	if f.DownloadArchive != nil {
		return f.DownloadArchive(cfg, sha)
	}
	return source.DownloadArchive(cfg, sha)
}

// RunPollCycle performs one ingestion pass: lists the source bucket, determines which
// commit SHAs have not yet been ingested, and processes them newest-first.
// Each successfully ingested commit is immediately persisted to the cursor so that a
// crash mid-cycle does not cause re-ingestion of already-processed commits.
// Pass a non-nil fns to override the GCS-facing functions (useful in tests).
func RunPollCycle(ctx context.Context, src *source.Config, store storage.ArtifactStore, log Logger, fns ...PollFuncs) error {
	f := PollFuncs{}
	if len(fns) > 0 {
		f = fns[0]
	}
	cursor, err := store.ReadCursor(ctx)
	if err != nil {
		return fmt.Errorf("read cursor: %w", err)
	}

	processed := make(map[string]struct{})
	if cursor != nil {
		for _, sha := range cursor.ProcessedCommits {
			processed[sha] = struct{}{}
		}
	}

	// ListCommits already returns entries sorted newest-first so the most
	// recent commits are available to CLI users as early as possible.
	allCommits, err := f.listCommits(src)
	if err != nil {
		return fmt.Errorf("list commits: %w", err)
	}

	var toProcess []source.CommitEntry
	for _, entry := range allCommits {
		if _, ok := processed[entry.SHA]; !ok {
			toProcess = append(toProcess, entry)
		}
	}

	if len(toProcess) == 0 {
		log.Info("no new archives to ingest")
		return nil
	}

	log.Info(fmt.Sprintf("found %d new commit(s) to ingest", len(toProcess)))

	for _, entry := range toProcess {
		if err := ctx.Err(); err != nil {
			return err
		}

		archiveData, err := f.downloadArchive(src, entry.SHA)
		if err != nil {
			log.Warn(fmt.Sprintf("failed to download %s: %v", shortSha(entry.SHA), err))
			continue
		}

		index, err := TransformAndStore(ctx, archiveData, entry.SHA, store, log)
		if err != nil {
			log.Warn(fmt.Sprintf("failed to ingest %s: %v", shortSha(entry.SHA), err))
			continue
		}

		if err := store.WriteIndex(ctx, entry.SHA, index); err != nil {
			log.Warn(fmt.Sprintf("failed to write index for %s: %v", shortSha(entry.SHA), err))
			continue
		}

		processed[entry.SHA] = struct{}{}

		committedShas := make([]string, 0, len(processed))
		for sha := range processed {
			committedShas = append(committedShas, sha)
		}
		sort.Strings(committedShas) // stable JSON order in cursor file

		newCursor := &storage.IngestionCursor{
			ProcessedCommits: committedShas,
			LastPollAt:       time.Now().UTC().Format(time.RFC3339),
		}
		if err := store.WriteCursor(ctx, newCursor); err != nil {
			log.Warn(fmt.Sprintf("failed to write cursor after %s: %v", shortSha(entry.SHA), err))
		}
	}

	return nil
}

// StartPolling launches a background goroutine that calls RunPollCycle at the given
// interval. The goroutine exits cleanly when ctx is cancelled. Each cycle runs after the
// previous one completes (non-overlapping), matching the Node.js setTimeout behaviour.
func StartPolling(ctx context.Context, src *source.Config, store storage.ArtifactStore, log Logger, interval time.Duration) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(interval):
				if err := RunPollCycle(ctx, src, store, log); err != nil {
					if ctx.Err() != nil {
						return
					}
					log.Warn(fmt.Sprintf("poll cycle error: %v", err))
				}
			}
		}
	}()
}
