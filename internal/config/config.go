// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.

package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

// DestinationType selects the backing store for ingested artifacts.
type DestinationType string

const (
	DestinationLocal DestinationType = "local"
	DestinationGCS   DestinationType = "gcs"
)

// Config holds all runtime configuration derived from environment variables.
type Config struct {
	Port                    int
	SourceBucket            string
	SourcePrefix            string
	DestinationType         DestinationType
	DestinationPathOrBucket string
	PollInterval            time.Duration
}

// Load reads configuration from environment variables and returns a validated Config.
func Load() (*Config, error) {
	port := 3081
	if v := os.Getenv("PORT"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid PORT %q: %w", v, err)
		}
		port = n
	}

	sourceBucket := "ci-typescript-archives"
	if v := os.Getenv("SOURCE_GCS_BUCKET"); v != "" {
		sourceBucket = v
	}

	sourcePrefix := "ts_type_check"
	if v := os.Getenv("SOURCE_GCS_PREFIX"); v != "" {
		sourcePrefix = v
	}

	destType := DestinationLocal
	if v := os.Getenv("DESTINATION_TYPE"); v != "" {
		destType = DestinationType(v)
	}

	destPath := os.Getenv("DESTINATION_PATH")
	if destPath == "" {
		destPath = os.Getenv("DESTINATION_BUCKET")
	}
	if destPath == "" {
		hint := "Set DESTINATION_PATH to a directory (e.g. ./data/artifacts)."
		if destType == DestinationGCS {
			hint = "Set DESTINATION_BUCKET to a GCS bucket name."
		}
		return nil, errors.New("missing destination config. " + hint)
	}

	pollIntervalMs := 10 * 60 * 1000
	if v := os.Getenv("POLL_INTERVAL_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			pollIntervalMs = n
		}
	}

	return &Config{
		Port:                    port,
		SourceBucket:            sourceBucket,
		SourcePrefix:            sourcePrefix,
		DestinationType:         destType,
		DestinationPathOrBucket: destPath,
		PollInterval:            time.Duration(pollIntervalMs) * time.Millisecond,
	}, nil
}
