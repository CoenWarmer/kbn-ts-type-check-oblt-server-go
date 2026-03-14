// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.

// Package server implements the HTTP server and route handlers.
package server

import (
	"fmt"
	"net/http"

	"kbn-ts-type-check-oblt-server-go/internal/source"
	"kbn-ts-type-check-oblt-server-go/internal/storage"
)

// Logger is the minimal logging interface used by the server.
type Logger interface {
	Info(msg string)
	Warn(msg string)
}

// New creates and returns a configured *http.Server. It does not start listening.
func New(port int, store storage.ArtifactStore, src *source.Config, log Logger) *http.Server {
	mux := http.NewServeMux()

	h := &handler{store: store, src: src, log: log}

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		handleGetHealth(w, r)
	})

	mux.HandleFunc("/artifacts", func(w http.ResponseWriter, r *http.Request) {
		log.Info(fmt.Sprintf("%s %s", r.Method, r.URL.Path))
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		h.handlePostArtifacts(w, r)
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
	})

	return &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}
}
