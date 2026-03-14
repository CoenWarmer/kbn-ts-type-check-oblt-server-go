# kbn-ts-type-check-oblt-server-go

Standalone cache service for granular TypeScript type-check artifacts. It **polls** a GCS bucket for new monolithic archives (produced by an external CI, e.g. Kibana), **transforms** them into a granular index + content-addressed artifact store, and **serves** on-demand artifact tars via HTTP. It does not depend on the Kibana repo.

- **Local:** Run with `DESTINATION_TYPE=local` and `DESTINATION_PATH=./data/artifacts` to store artifacts on the filesystem.
- **GCP:** Run with `DESTINATION_TYPE=gcs` and `DESTINATION_BUCKET=your-bucket` to store artifacts in a GCS bucket.

## Features

- **True parallelism**: all CPU cores for gzip/gunzip (no libuv thread-pool cap).
- **Streaming pipeline**: `io.Pipe` and tee eliminate buffering during passthrough and selective restores.
- **Goroutine concurrency**: handles many concurrent clients without event-loop saturation.

## Behaviour

- **Cache hit, no project filter:** Streams the full monolithic archive directly from the **source** GCS bucket to the client (passthrough). No local artifact read; fastest path when the client wants everything.
- **Cache hit, with project filter:** Serves a **framed stream** (`Content-Type: application/x-artifact-stream`) of individual project `.tar.gz` blobs. Each blob is prefixed with a 4-byte big-endian length. Response includes `X-Project-Count` header with the number of unique blobs. The client can pipeline download and extraction with no extra buffering.
- **Cache miss:** Tries to stream the monolithic archive from the source GCS bucket. If found, **passthrough-streams** it to the client while **teeing** the bytes for asynchronous on-demand ingestion. When the response finishes, the captured bytes are transformed and written to the store (index + artifacts + cursor). If the source has no archive, responds with `404`.

The store interface supports **raw archive** storage (`WriteRawArchive`); the current handler does not persist a raw copy on cache miss (only the granular index + artifacts are written).

## Config (env)

| Env | Description | Default |
|-----|-------------|---------|
| `PORT` | HTTP server port | `3081` |
| `SOURCE_GCS_BUCKET` | Source bucket to poll (and to stream from on cache miss / full restore) | `ci-typescript-archives` |
| `SOURCE_GCS_PREFIX` | Prefix inside the bucket (e.g. `ts_type_check`) | `ts_type_check` |
| `DESTINATION_TYPE` | `local` or `gcs` | `local` |
| `DESTINATION_PATH` | For local: directory path for index, artifacts, and raw archives | (required if local) |
| `DESTINATION_BUCKET` | For GCS: bucket name | (required if gcs) |
| `POLL_INTERVAL_MS` | How often to poll for new archives (ms) | `600000` (10 min) |

## API

- **GET /health** — Health check. Returns `{"status":"ok","service":"kbn-ts-type-check-oblt-server"}`.
- **POST /artifacts** — Request artifacts. Body: `{ "commitSha": "<sha>", "projects": ["packages/foo/tsconfig.json", ...] }`.
  - If `projects` is **omitted**: response is the full monolithic archive streamed from GCS (`application/gzip`). Uses passthrough (no project filtering).
  - If `projects` is **provided** and the commit is in the cache: response is `application/x-artifact-stream` — a sequence of `[4-byte length][blob bytes]` for each requested project blob.
  - If the commit is not in the cache: same as omitted — attempts GCS passthrough (and on success runs on-demand ingest in the background).

## Requirements

- **Go 1.26+** (see `go.mod`).
- For GCS: Application Default Credentials (`gcloud auth application-default login`).

## Build and run

**Build binary:**

```bash
go mod tidy
make build
```

**Run (local storage):**

```bash
make run
# or
DESTINATION_TYPE=local DESTINATION_PATH=./data/artifacts ./bin/server
```

**Run (GCS destination):**

```bash
DESTINATION_TYPE=gcs DESTINATION_BUCKET=my-oblt-cache-bucket ./bin/server
```

**Tests:**

```bash
make test
# or
go test ./...
```
