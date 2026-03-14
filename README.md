# kbn-ts-type-check-oblt-server-go

Go port of the TypeScript type-check artifact cache server. Polls a GCS bucket for monolithic
archives, transforms them into a granular content-addressed store, and serves on-demand artifact
tars over HTTP.

## Why Go?

- **True parallelism**: all CPU cores for gzip/gunzip (no libuv thread-pool cap).
- **Streaming pipeline**: `io.Pipe` eliminates buffering during selective restores.
- **Goroutine concurrency**: handles 30+ concurrent clients without event-loop saturation.

## Config (env vars)

| Env                   | Description                                    | Default                    |
|-----------------------|------------------------------------------------|----------------------------|
| `PORT`                | HTTP server port                               | `3081`                     |
| `SOURCE_GCS_BUCKET`   | Source bucket to poll for monolithic archives  | `ci-typescript-archives`   |
| `SOURCE_GCS_PREFIX`   | Prefix inside the bucket                       | `ts_type_check`            |
| `DESTINATION_TYPE`    | `local` or `gcs`                               | `local`                    |
| `DESTINATION_PATH`    | For local: directory for index + artifacts     | *(required if local)*      |
| `DESTINATION_BUCKET`  | For GCS: destination bucket name               | *(required if gcs)*        |
| `POLL_INTERVAL_MS`    | How often to poll for new archives (ms)        | `600000` (10 min)          |

## API

- **GET /health** — Health check. Returns `{"status":"ok","service":"kbn-ts-type-check-oblt-server"}`.
- **POST /artifacts** — Request artifacts.
  Body: `{"commitSha":"<sha>","projects":["packages/foo/tsconfig.json",...]}`.
  If `projects` is omitted, returns a full GCS passthrough. Response: `application/gzip` tar stream.

## Build and run

```bash
# Install dependencies (requires Go 1.22+)
go mod tidy

# Build binary
make build

# Run locally with filesystem storage
DESTINATION_TYPE=local DESTINATION_PATH=./data/artifacts make run

# Or directly
DESTINATION_TYPE=local DESTINATION_PATH=./data/artifacts ./bin/server
```

Example with GCS destination:

```bash
DESTINATION_TYPE=gcs DESTINATION_BUCKET=my-oblt-cache-bucket ./bin/server
```

GCS authentication uses Application Default Credentials (`gcloud auth application-default login`).
