// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.

package ingestion

import (
	"testing"
)

func TestToProjectPath(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "target/types path maps to project root",
			input: "packages/foo/target/types/bar.d.ts",
			want:  "packages/foo/tsconfig.json",
		},
		{
			name:  "deeply nested target/types path",
			input: "x-pack/plugins/apm/target/types/client/index.d.ts",
			want:  "x-pack/plugins/apm/tsconfig.json",
		},
		{
			name:  "tsconfig variant file maps to project root",
			input: "packages/foo/tsconfig.type_check.json",
			want:  "packages/foo/tsconfig.json",
		},
		{
			name:  ".tsbuildinfo file maps to project root",
			input: "packages/foo/tsconfig.tsbuildinfo",
			want:  "packages/foo/tsconfig.json",
		},
		{
			name:  "deeply nested non-target file maps to its directory",
			input: "packages/foo/src/index.ts",
			want:  "packages/foo/src/tsconfig.json",
		},
		{
			name:  "top-level file maps to root tsconfig",
			input: "tsconfig.json",
			want:  "tsconfig.json",
		},
		{
			name:  "scoped package path",
			input: "packages/@kbn/core/target/types/index.d.ts",
			want:  "packages/@kbn/core/tsconfig.json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := toProjectPath(tt.input)
			if got != tt.want {
				t.Errorf("toProjectPath(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestGroupByProject(t *testing.T) {
	entries := []fileEntry{
		{relativePath: "packages/foo/target/types/index.d.ts"},
		{relativePath: "packages/foo/tsconfig.json"},
		{relativePath: "packages/bar/target/types/bar.d.ts"},
		{relativePath: "packages/bar/tsconfig.tsbuildinfo"},
	}

	result := groupByProject(entries)

	if len(result) != 2 {
		t.Fatalf("expected 2 projects, got %d", len(result))
	}

	fooFiles, ok := result["packages/foo/tsconfig.json"]
	if !ok {
		t.Error("expected project packages/foo/tsconfig.json")
	}
	if len(fooFiles) != 2 {
		t.Errorf("expected 2 files for packages/foo, got %d", len(fooFiles))
	}

	barFiles, ok := result["packages/bar/tsconfig.json"]
	if !ok {
		t.Error("expected project packages/bar/tsconfig.json")
	}
	if len(barFiles) != 2 {
		t.Errorf("expected 2 files for packages/bar, got %d", len(barFiles))
	}
}

func TestGroupByProject_EmptyInput(t *testing.T) {
	result := groupByProject(nil)
	if len(result) != 0 {
		t.Errorf("expected empty map for nil input, got %d entries", len(result))
	}
}
