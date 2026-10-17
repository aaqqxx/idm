# Internet Download Manager

Internet Download Manager is a Go-based application designed to manage and facilitate the downloading of files from the internet efficiently. This project is designed with a scale-up capability in mind, ensuring that it can handle increased workload and demand as needed.

## Features

- Easy-to-use interface for managing download tasks.
- Support for downloading files from various sources.
- Efficient download management with resume functionality.
- Detailed download progress tracking and status updates.
- Cross-platform compatibility.

## Segmented download engine

The transfer core (`internal/download`) is a multi-connection segmented downloader. Its
scheduling model follows the algorithm recovered by reverse-engineering Internet
Download Manager 6.43 with Ghidra (the `ChunksList` / `Session` / optional-connection
controller). The constants below are the actual values from that binary.

### How it works

1. **Probe.** A `HEAD` request, falling back to a one-byte `Range: bytes=0-0` GET,
   learns the object size, whether the server honours `Range` (206), and the
   `ETag` / `Last-Modified` validators needed for a safe resume. A server that
   ignores `Range` transparently degrades to a single ordered connection.
2. **Plan.** Parallelism is `remaining / min_chunk`, clamped by the user ceiling and
   the hard cap. With the default 10 KiB floor this is exactly IDM's arithmetic:
   `n = remaining / 0x2800`, `n = min(n, userMax)`, `n = min(n, 0x20)`.
3. **Execute.** A worker pool claims segments, each connection writing at its own
   file offset through `io.WriterAt`. When the pool wants to widen, `EnsureSpans`
   splits the largest *unowned* span, so the segment map grows at runtime instead of
   being fixed up front.
4. **Watchdog.** A connection that produces no bytes for `idle_timeout` (30 s,
   matching IDM) is cancelled and its span is retried on another slot.
5. **Checkpoint.** The segment table is persisted atomically; a validator mismatch
   on restart discards the checkpoint rather than risking silent corruption.

### Why it beats a naive threaded downloader

| Naive downloader | This engine |
|---|---|
| Fixed `size/N` split at start | Span map widens at runtime via `EnsureSpans` |
| One thread per `Range`, forever | Slots claim/split/steal spans as they free up |
| A stall fails the transfer | Per-connection idle watchdog + retry budget |
| Resume replays from scratch | Validated checkpoint resumes at exact offsets |
| Unbounded connections | `min(remaining/10KiB, userMax, 32)` |
| Silent overwrite of existing files | `no_overwrite` picks `name (1).ext` |
| Half-written file under the real name | Bytes stage in `.part`, renamed only on success |
| No bandwidth control | Shared token bucket caps aggregate rate |
| Fragmented on out-of-order writes | Full size preallocated before transfer starts |
| Can't download Referer/Cookie/authed links | `Request` options: headers, Referer, cookies, Basic Auth |

### Tuning

```yaml
download:
  engine:
    max_connections: 16      # user ceiling
    hard_cap: 32             # absolute cap (IDM: 0x20)
    min_chunk_size: 10240    # one connection per 10 KiB (IDM: 0x2800)
    min_work_unit: 262144    # smallest runtime split
    idle_timeout: "30s"      # stall watchdog (IDM: 0x1e seconds)
    max_retries: 5
    buffer_size: 131072
    enable_resume: true
    state_directory: ""      # default: <download_dir>/.checkpoints
    max_bytes_per_second: 0  # aggregate cap; 0 = unlimited
    no_overwrite: true       # never clobber an existing file
    disable_preallocate: false
    # Request customisation for gated downloads:
    referer: ""              # CDN Referer check
    headers: {}              # extra headers (Range/Content-Length rejected)
    cookies: []              # ["session=abc123"]
    username: ""             # HTTP Basic Auth
    password: ""
```

### Tests

```bash
go test ./internal/download/ -race

# live check against a real server:
IDM_LIVE_URL=https://example.com/bigfile.bin go test ./internal/download -run TestLiveDownload -v

# verify the throttle end to end (rate stays near the cap):
IDM_LIVE_URL=https://example.com/bigfile.bin IDM_LIVE_LIMIT=2000000 \
  go test ./internal/download -run TestLiveDownload -v
```

## Installation

To install Internet Download Manager, follow these steps:

1. Clone the repository:

```bash
git clone https://github.com/maxuanquang/idm.git
```

2. Navigate to the project directory:

```bash
cd idm
```

3. Build project:

```bash
make build
```

4. Start all necessary services:

```bash
make docker-compose-prod-up
```

5. After all services have started up, we can start the project:

```bash
make run
```

## Usage

Once installed, you can use Internet Download Manager to efficiently manage your download tasks. The application provides a user-friendly interface for adding, resuming downloads.

To start using Internet Download Manager:

1. Access the HTTP server at `localhost:8081`.
2. Use the interface to add download tasks by providing the URL of the file you want to download.
3. Monitor the progress of your downloads and manage them as needed.
