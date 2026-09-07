# Install — workflow-data-collector

The install unit is **one static binary** (`bin/wfdc`) plus **one config file**
(`wfdc.toml`). No factory changes, no rebuild, no system dependencies — the
binary is a static musl executable that runs on any Linux host or container.

A factory "install" means: place the binary, point it at the factory's Redis
event stream, run it. It then tails the stream and writes analyst-ready
datasets to a local directory.

---

## 1. Get the binary

Download the binary and its checksum (from `main`; pin to a release tag for a
fixed version):

```bash
BASE=https://raw.githubusercontent.com/camorazrushimoe/plugins/main/workflow-data-collector
curl -fsSL "$BASE/bin/wfdc"      -o wfdc
curl -fsSL "$BASE/bin/SHA256SUMS" -o SHA256SUMS
chmod +x wfdc
```

Verify integrity (recommended):

```bash
sha256sum -c SHA256SUMS        # must print: wfdc: OK
./wfdc --version               # sanity: prints "wfdc <version>"
```

## 2. Configure

Create `wfdc.toml` next to the binary. The only value that varies per factory
is `redis_url`:

```toml
redis_url = "redis://127.0.0.1:6380"
stream    = "office:events"
data_dir  = "./wfdc-data"
max_mb    = 500
```

**Which `redis_url`?**

| How you run it | `redis_url` |
|---|---|
| On the factory host, next to agent-office | `redis://127.0.0.1:6380` (office default host mapping) |
| As a sidecar inside the factory docker network | `redis://shared-memory:6379` |
| Password-protected Redis | `redis://:PASSWORD@host:port` |

`stream` must be the durable event stream (`office:events` is the office
default). Do **not** point it at the pub/sub topic (`office:events:topic`).

## 3. Smoke test (one shot)

```bash
./wfdc --once --config wfdc.toml     # read one batch, flush, CHECKPOINT, exit 0
./wfdc status --json                 # prints live state; exit 0 = healthy
```

`--once` exits 0 after one batch. If it errors, the cause is almost always the
Redis URL, stream name, or network reachability (see Troubleshooting).

## 4. Run continuously (daemon)

Example systemd unit `/etc/systemd/system/wfdc.service`:

```ini
[Unit]
Description=Workflow Data Collector
After=network-online.target

[Service]
ExecStart=/opt/wfdc/wfdc follow --config /opt/wfdc/wfdc.toml
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload && sudo systemctl enable --now wfdc
```

The default command is `follow`: it blocks on `XREAD` and writes the dataset as
events arrive. `SIGTERM`/`SIGINT` flush and exit 0 (clean stop). Alternatively
run it under Docker, a container supervisor, or any process manager.

## 5. Where the data lands

Under `data_dir`:

| Path | What |
|---|---|
| `raw/events.jsonl` | the raw event dataset (one JSON line per event) |
| `sessions/sessions.jsonl` | paired agent sessions |
| `teams/` | discovered teams |
| `MANIFEST.json` | run metadata: version, event/session counts, checksums, disk bytes |
| `DROP_LOG.json` | disk-cap drops (present only if `max_mb` was enforced) |
| `CHECKPOINT` | resume point (stream id), for restart-without-duplicates |

`./wfdc status --json` always re-derives live state and is byte-stable across
runs (same snapshot → same output).

## 6. Config reference

`wfdc.toml` keys: `redis_url`, `stream`, `data_dir`, `max_mb`.

CLI flags (override TOML, then env): `--config`, `--redis`, `--stream`,
`--max-mb`, `--expire-after`, `--once`, `--max-reads N`, `--max-idle-ms MS`,
`--version`.

Env fallbacks: `WFDC_CONFIG`, `WFDC_REDIS_URL`, `WFDC_STREAM`, `WFDC_MAX_MB`.

## Troubleshooting

- `config error: unknown argument "status"` → stale binary; re-download from §1.
- `connection refused` → wrong `redis_url`, or Redis isn't exposed to your host.
- `NOAUTH` → Redis needs a password; put it in `redis_url` (`redis://:pass@host:port`).
- `max_mb` reached → oldest days are trimmed first; raise `max_mb` if you need a longer window.

## Rebuilding from source (optional)

The binary is reproducible. `workflow-data-collector/build.sh` builds it with a
pinned toolchain (`rustup 1.98.0` + `x86_64-unknown-linux-musl`) and records the
sha256; `verify-ship-unit.sh` checks static linking + sha256 + version.
