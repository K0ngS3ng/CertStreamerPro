# CertStreamerPro

CertStreamerPro is a fully self-hosted Certificate Transparency (CT) monitoring system built in Go. It discovers all public CT logs (excluding test/pending/rejected), tails them in real time, extracts CN/SAN DNS names, normalizes subdomains using the Public Suffix List, deduplicates globally, and emits structured JSON events.

## Features

- Dynamic CT log discovery (no hardcoded logs)
- Per-log tailing via `get-sth` + `get-entries`
- X.509 parsing from CT entries (leaf + extra_data)
- CN + SAN extraction
- PSL-aware subdomain filtering (multi-stage TLDs supported)
- In-memory dedup (Bloom + map)
- In-memory progress tracking per log
- Lossless delivery under backpressure (pending queue)
- Structured JSON output with SCT timestamp + latency
- Graceful shutdown

## Build

```bash
go mod tidy
go build -o CertStreamerPro ./cmd/CertStreamerPro
```

## Run

```bash
./CertStreamerPro -config config.yaml
```

## Example Output

```json
{"domain":"staging.example.co.in","source_log":"https://ct.googleapis.com/logs/us1/argon2027h1","observed_at":"2026-02-03T17:51:49.642112Z","sct_timestamp":"2026-02-03T16:14:39.566Z","latency_ms":5830076}
```

## Backpressure Behavior

To guarantee no loss, the normalization stage will block if the dedup stage is saturated. This keeps correctness intact but can slow ingestion under extreme load. If you need both non-blocking ingestion and zero loss, add a disk-backed overflow queue at the normalization stage.

## Configuration

See `config.yaml` for defaults. Important options:

- `include_all_logs`: use all public logs (excluding test/pending/rejected)
- `allowed_operators`: optional operator allowlist (names must match the operator names in the CT log list)
- `only_subdomains`: enforce PSL subdomain filtering
- `dedup_workers`, `dedup_batch_size`, `dedup_flush_interval`
- `psl_cache_size`

### Allowed Operators

This project can handle **any operator** present in the CT log list. Operator names are **not hardcoded** and come directly from:\n
`https://www.gstatic.com/ct/log_list/v3/log_list.json`

To allow a subset, set `allowed_operators` to the exact operator names from the log list. Example:

```yaml
allowed_operators:
  - "Google"
  - "Cloudflare"
  - "Let's Encrypt"
  - "DigiCert"
  - "Sectigo"
  - "TrustAsia"
```

## SDK Usage (Go)

```go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/K0ngS3ng/CertStreamerPro/pkg/ctmonitor"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg := ctmonitor.DefaultConfig()
cfg.DataDir = "./data" // spill queue directory
	cfg.OnlySubdomains = true
	cfg.AllowedOperators = []string{"Google", "Cloudflare", "Let's Encrypt", "DigiCert"}

	mon, err := ctmonitor.Start(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer mon.Close()

	for ev := range mon.Events {
		log.Printf("%s %s %s", ev.Domain, ev.LogURL, ev.Observed.UTC().Format(time.RFC3339Nano))
	}
}
```

## Integration Notes

When integrating into another service, keep these in mind:

1. **Spill queue directory is required.**  
   `DataDir` is used only for the spill queue. It can be a small local disk path.

2. **Memory‑only dedup.**  
   Dedup and progress reset on restart. Your app may re‑emit domains after a restart and will resume from current STH (gaps during downtime are possible).

3. **Backpressure behavior.**  
   Ingestion is lossless. If dedup is saturated, events will be written to the spill log and replayed when capacity returns.

4. **Shutdown.**  
   Always cancel the context and call `mon.Close()` so goroutines exit cleanly.

## Notes on Timestamps

- `sct_timestamp` is the CT log inclusion time from `leaf_input`.
- `observed_at` is when this tool observed the entry.
- `latency_ms` is `observed_at - sct_timestamp`.

## License

No license specified yet.

## Important: In-Memory Mode + Spill Queue

This build runs **without any on-disk database**. It uses memory for dedup and progress, plus a small disk-backed **spill queue** for overflow when channels are saturated. As a result:

- **Dedup is not persistent** across restarts.
- **Progress is not persisted**, so restarts will resume from current STH and may skip older entries.
- **No BadgerDB lock errors**, and minimal disk usage (spill queue only).

If you need persistence across restarts, reintroduce a durable store.
