# Workflow Data Collector — Specification

**Plugin:** `workflow-data-collector`
**Binary:** `wfdc`
**Status:** Draft v0.2.2
**Approach:** spec-driven. Implementation follows this file.
**Ship unit:** one static `musl` binary + one config file next to it.
**Factory:** not modified. Drop the binary beside a running factory, point the config at Redis, start the process.

---

## 1. Job

Read the factory Redis stream in near-real-time and write a raw,
minimally structured dataset to disk. Lab owns analysis, conclusions,
and any later redesign of the collector.

v1 does two things only:

1. Persist every bus event as JSONL (the raw dataset).
2. Pair `task.started` / `task.finished` into session rows (the
   structured layer).

Workflow interpretation, blocker labels, “unnecessary session”
verdicts, and chain mining stay in Lab.

---

## 2. Install model

```
<factory-host>/
  wfdc                 # compiled musl binary
  wfdc.toml            # config next to the binary
  wfdc-data/           # output (created on first run)
```

No factory rebuild, no hook change, no compose change required.
The binary is a sidecar: it opens Redis as a client and writes files.

```toml
# wfdc.toml — search order: --config, $WFDC_CONFIG, <binary-dir>/wfdc.toml, ./wfdc.toml
redis_url = "redis://127.0.0.1:6380"
stream    = "office:events"
data_dir  = "./wfdc-data"
max_mb    = 500
```

`redis_url` is how a factory on a non-default port is targeted.
Host, port, password, db index all live in the URL.

A **relative** `data_dir` is resolved against the directory of the
config file that supplied it; if no config file was used, against the
binary's directory. Not against the process cwd (cron/systemd often
start in `/`).

`max_mb` is the hard cap on collected JSONL under `data_dir`.
Default **500**. Any positive integer is allowed (1000, 2000, …).
Env `WFDC_MAX_MB` and flag `--max-mb` override the file.
Values below 16 are treated as 16 so one flush cannot livelock.

CLI overrides the file when passed:

```
./wfdc                  # follow, using wfdc.toml
./wfdc --config /path/wfdc.toml
./wfdc --redis redis://127.0.0.1:6379 --stream office:events --max-mb 1000
```

Env vars (`WFDC_REDIS_URL`, `WFDC_STREAM`, `WFDC_DATA_DIR`,
`WFDC_CONFIG`, `WFDC_MAX_MB`) also work. Precedence: CLI > env >
config file > built-in defaults (`redis://127.0.0.1:6380`,
`office:events`, `./wfdc-data`, `500`).

---

## 3. Runtime

- Default command is **follow**: blocking `XREAD` on the stream
  (`BLOCK` ~1000 ms, `COUNT` small). No busy-loop.
- Restart: read `$data_dir/CHECKPOINT` and `XREAD` from that stream
  id. Events that landed in the stream while the process was dead
  **are** consumed on resume, as long as Redis still has them.
  Automatic catch-up is the checkpoint. `wfdc backfill` is only for
  a chosen range (first install, rebuilt derived tables, or a
  trimmed stream you recovered elsewhere).
- A hole in `raw/` appears only when events never reached the
  stream (hook swallowed a down bus), the stream was trimmed past
  the checkpoint, or Redis lost data. Do not invent rows to fill it.
- If Redis is down or the stream key is missing: log, wait, retry
  with backoff. Do not exit.
- One writer per `data_dir`. Lock file `$data_dir/.lock` holds pid +
  started_at. A lock whose pid is not running is stale and is taken
  over. A live foreign pid → exit code 3.
- After every successful flush, enforce `max_mb` (§5.5).
- Lightweight: a few MB RSS, near-zero CPU when the bus is quiet.

```
wfdc                 # follow
wfdc follow
wfdc backfill [--from STREAM_ID] [--to STREAM_ID]
wfdc status          # checkpoint, last flush, redis reachable, bytes used / cap?
```

---

## 4. What is read

Source of truth: Redis **STREAM** at `stream` from the config
(Office default: `office:events`). Pub/sub is ignored.

### 4.1 Wire format (this is what XREAD actually returns)

A stream entry is a flat list of string fields, not a nested JSON
document. `crew/office-log.py` and `crew/publish-event.py` treat
these keys as first-class fields:

`action`, `actor`, `target`, `timestamp`, `team`, `project`,
`summary`, plus whatever else the publisher added.

`office/activity.py` builds a logical envelope (`id`, `actor`,
`action`, `target`, `timestamp`, `team`, `payload{…}`) via
`make_envelope` / `publish_event`. On the wire that envelope is
still string fields. Nested objects (`payload`, `task_ref`) are
either:

- a JSON string in one field (`payload`, `json`, or `envelope`), or
- flattened (`snippet`, `session_id`, `summary`, …) next to `action`.

v1 decoder, in order:

1. Read all string fields on the entry.
2. If `json` or `envelope` is valid JSON object, use it as the
   envelope and overlay any missing top-level fields from the flat
   map.
3. If `payload` is a JSON object string, parse it; otherwise treat
   known payload keys sitting at the top level (`session_id`,
   `snippet`, `summary`, `task_ref`, `handoff`) as the payload.
4. `task_ref` / `handoff` may themselves be JSON strings.

Unknown fields are kept under `fields` on the raw JSONL line so
Lab can see what the factory actually sent. The plugin never
requires a factory schema change.

### 4.2 Actions used for pairing

- `task.started`  — agent:start hook
- `task.finished` — agent:end hook

Every other `action` is stored raw and ignored by the assembler.

Known payload keys when present: `session_id`, `snippet`, `summary`,
`task_ref` (`issues` / `prs` / `linear`), `handoff` (finish only).
Missing keys stay null.

---

## 5. Output on disk

JSONL only. No CSV, no Parquet in v1.

```
$data_dir/
  MANIFEST.json
  CHECKPOINT
  raw/
    dt=YYYY-MM-DD/events.jsonl
  teams/
    <team_safe>/
      raw/dt=YYYY-MM-DD/events.jsonl
      sessions/dt=YYYY-MM-DD/sessions.jsonl
```

- `raw/` is the dataset. Append-only within a partition. One JSON
  object per line. Oldest line is at the **start** of the file;
  new lines are appended at the end.
- `teams/<team_safe>/` is the same events split by `team`.
- `sessions/` is the only derived table in v1.
- `dt=` is the UTC date of `timestamp`, falling back to the Redis
  stream-id millisecond clock if `timestamp` is unparsable.

### 5.1 Team folder name

`team` from the bus is not a safe path.

1. Empty `team`: `_office` if `actor` is one of
   `architect`, `staff-engineer`, `scrum-master`, `super-devops`,
   `lifecycle`, `system`; otherwise `_unknown`.
2. Sanitize: keep `[A-Za-z0-9._-]`, collapse anything else to `_`,
   strip leading dots, cap at 64 chars. Empty after that → `_unknown`.
3. Keep the original `team` string on every JSONL row.

### 5.2 Raw event line

```json
{
  "stream_id": "1725062400000-0",
  "envelope_id": null,
  "ts": "2026-08-30T21:00:00Z",
  "actor": "developer",
  "action": "task.started",
  "target": "developer",
  "team": "dev-1",
  "project": null,
  "payload": {},
  "fields": {}
}
```

`fields` is the raw flat map from Redis (strings). `payload` is
whatever decoded as payload. `envelope_id` is envelope `id` when
present.

### 5.3 Session line

A session is one **agent turn**: one `task.started` paired with a
later `task.finished`.

Hermes `session_id` is a conversation id, not a turn id. Several
starts for the same agent can share one `session_id`. **Do not**
use `(team, actor, session_id)` as the primary pairing key — that
would collapse consecutive turns.

**Pairing (FIFO per agent):**

1. Bucket unmatched starts by `(team, actor)`.
2. A `task.finished` attaches to the oldest unmatched start in that
   bucket whose `session_id` is compatible: both empty, or equal.
   If none match, the finish is `orphan_finish`.
3. A new `task.started` while that agent already has an unmatched
   start marks the previous row `interrupted` and opens a new one.

`session_pk` = sha256 of `team|actor|start_stream_id`.
Stable across rebuilds of the derived table from the raw that is
still on disk.

| Field | Meaning |
|-------|---------|
| `session_pk` | hash above |
| `team`, `actor`, `session_id` | as on the start (finish may fill a missing session_id) |
| `start_stream_id`, `finish_stream_id` | finish null while open |
| `started_at`, `finished_at` | |
| `duration_ms` | null while open |
| `state` | `completed` \| `open` \| `interrupted` \| `orphan_finish` \| `expired` |
| `snippet_in`, `snippet_out` | start / finish snippet |
| `issues`, `prs`, `linear` | union of start+finish refs |
| `handoff` | from finish |
| `project` | if present on either envelope |

States:

- `completed` — start and compatible finish, in stream-id order
- `open` — start seen, no finish yet
- `interrupted` — a newer start for the same `(team, actor)` arrived
  first
- `orphan_finish` — finish with no compatible unmatched start
- `expired` — `open` longer than 6 hours (killed container; hooks
  do not fire on OOM / `docker stop`)

Open rows live on the `dt=` of `started_at` and are upserted when
they close, even after midnight. Upsert = rewrite that day's
`sessions.jsonl` via `*.tmp` + atomic rename.

This table exists so Lab does not re-join start/stop on every dump.
It is not a workflow model.

### 5.4 MANIFEST.json

Rewritten each flush. Must **not** store the Redis password.
Write `redis_url` with userinfo stripped (`redis://127.0.0.1:6380`,
never `redis://:secret@…`). Also: plugin version, stream name,
checkpoint, event/session counts, discovered original team strings,
`bytes_used`, `max_mb`.

### 5.5 Disk cap (`max_mb`)

Purpose: a forgotten sidecar must not fill the factory disk.

**What counts toward the cap.** All `*.jsonl` under `data_dir`
(office `raw/`, per-team `raw/`, `sessions/`).
`MANIFEST.json`, `CHECKPOINT`, and `.lock` do not count.

**When.** After every successful flush (and at start of `follow`,
in case the cap was lowered).

**How old data leaves.** JSONL is append-only, so the oldest events
are the earliest lines / the oldest `dt=` folders — not the tail.

1. Sum JSONL bytes. If `<= max_mb * 1024 * 1024`, stop.
2. While over the cap and more than one `dt=` date exists: delete
   the **oldest** date everywhere it appears (`raw/dt=…` and every
   `teams/*/raw/dt=…`, `teams/*/sessions/dt=…`). Remove empty
   parent dirs. Log one line per dropped date.
3. If only today's date remains and it is still over the cap: trim
   each of today's JSONL files from the **start** (drop oldest
   complete lines) until the tree is under the cap, or the file
   would have fewer than one line left. Rewrite via `*.tmp` +
   atomic rename. Never emit a partial JSON line.

Checkpoint is **not** moved backward. Dropped rows are a visible
gap for Lab (`status` reports `bytes_used` vs cap). Redis itself
is not trimmed.

`0` or a missing key → default 500. Negative → default 500.

---

## 6. Gaps and honesty

- A hole is a jump in `stream_id` / time, or `status` showing a
  checkpoint older than `XINFO STREAM` last-id. Nothing is
  synthesized to fill it. Hitting `max_mb` also drops the oldest
  on-disk rows; that is intentional, not a collector crash.
- Blockers are not labelled. Hooks do not emit `blocked`.
- Snippets are whatever the factory already put on the bus
  (first 200 characters). Stored as-is.
- Model name, tokens, full prompts are not on the bus today.

---

## 7. Determinism

Same stream range + these pairing rules → same `session_pk` values,
for the raw that is still on disk. Within a partition, raw files
are append-only until the cap trims the oldest date or the start
of today's file.

---

## 8. Non-goals (v1)

- Changing Agent Office or any team template
- Pushing data off the box
- Dashboards
- CSV / Parquet
- LLM
- Reconstructing multi-agent chains
- Creating the Redis stream if it is missing
- Time-based retention (days). Cap is size-only.

---

## 9. Implementation order

1. Config load + path resolution + Redis client.
2. Follow + checkpoint + `raw/` writer (office-wide and per team).
3. Session pairing + `sessions.jsonl`.
4. `max_mb` enforcement after flush.
5. `MANIFEST.json` (redacted URL) + `status`.
6. `backfill`.
7. musl binary in `workflow-data-collector/bin/`.

---

## 10. Example config

See `wfdc.toml.example` in this directory.
