# Workflow Data Collector — Specification

**Plugin:** `workflow-data-collector`
**Binary:** `wfdc`
**Status:** Draft v0.3.0
**Approach:** spec-driven. Implementation follows this file.
**Ship unit:** one static `musl` binary + one config file next to it.
**Factory:** not modified. Drop the binary beside a running factory, point the config at Redis, start the process.

**Revision v0.3.0 (2026-08-31):** closes the blocking findings of the four
adversarial reviews on issue #5 (product, engineering, infra, testability).
Changed sections: §2 (perms, max_mb pin, new CLI flags), §3 (dedupe,
checkpoint durability, lock liveness, graceful stop, exit codes), §4.1
(decoder contract), §5.3 (pairing rules, orphan placement, expiry),
§5.4 (MANIFEST observability), §5.5 (deterministic trim, drop log).

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
  wfdc.toml            # config next to the binary (must be 0600)
  wfdc-data/           # output (created on first run, mode 0700)
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
binary's directory. A relative `WFDC_DATA_DIR` env value resolves
against the binary's directory (env has no config-file base). Never
against the process cwd (cron/systemd often start in `/`).

`max_mb` is the hard cap on collected JSONL under `data_dir`.
Default **500**. Any positive integer is allowed (1000, 2000, …).
Env `WFDC_MAX_MB` and flag `--max-mb` override the file.
`0`, a missing key, or a negative value → default 500.
Values 1–15 are treated as 16 so one flush cannot livelock.

**File permissions.** Snippets are raw agent-message fragments and
`task_ref` can carry issue/PR references. `wfdc-data/` is created
**0700**; every file inside it (`*.jsonl`, `CHECKPOINT`,
`MANIFEST.json`, `.lock`) is created **0600**. `wfdc.toml` holds the
Redis password and must be **0600** (the collector warns and exits 1
if it is group/world-readable).

CLI overrides the file when passed:

```
./wfdc                  # follow, using wfdc.toml
./wfdc --config /path/wfdc.toml
./wfdc --redis redis://127.0.0.1:6379 --stream office:events --max-mb 1000
./wfdc --once           # one XREAD batch, then clean stop (§3)
./wfdc --max-reads 10   # ten XREAD batches, then clean stop
./wfdc --max-idle-ms 5000  # stop when no event arrives for 5 s
./wfdc --expire-after 6 # expiry window in hours (default 6, test knob)
./wfdc status --json    # machine-readable status (§5.4)
```

Env vars (`WFDC_REDIS_URL`, `WFDC_STREAM`, `WFDC_DATA_DIR`,
`WFDC_CONFIG`, `WFDC_MAX_MB`, `WFDC_EXPIRE_HOURS`) also work.
Precedence: CLI > env > config file > built-in defaults
(`redis://127.0.0.1:6380`, `office:events`, `./wfdc-data`, `500`,
expiry 6 h). Prefer `WFDC_REDIS_URL` over `--redis` for credentialed
URLs (argv is visible in `/proc/<pid>/cmdline` and shell history).

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
  with backoff (1 s → 60 s, capped, with jitter; reset on success).
  Do not exit. CHECKPOINT never advances while disconnected.

### 3.1 Flush, checkpoint, and at-least-once dedupe

Deliveries from the stream are **at-least-once**: a crash between
consuming a batch and persisting the checkpoint makes resume re-read
the same entries. The dedupe rule makes that harmless:

- **One flush per XREAD batch.** Append the batch to all views
  (office `raw/` and each `teams/*/raw/`), fsync the JSONL files,
  then rewrite `CHECKPOINT` **atomically** (`CHECKPOINT.tmp` +
  rename) and fsync it. Only after CHECKPOINT is durable does the
  next XREAD advance. Order: write batch → fsync JSONL → atomic
  CHECKPOINT → advance the in-memory watermark.
- **Dedupe at write time:** skip any entry whose `stream_id` is
  `<=` the last flushed checkpoint. Stream ids are monotonic per
  stream, so re-reads after a crash between consume and checkpoint
  cannot duplicate rows in `raw/` (or anywhere else).
- CHECKPOINT stores the last flushed stream id. It is never moved
  backward and never advanced for un-flushed data.

### 3.2 Startup repair

A crash mid-append can leave a partial JSON line at EOF. On startup,
before reading: scan every JSONL file; if the last line does not end
with `\n`, truncate the partial line (drop it) and log the repair
(file + bytes dropped). Lab never sees a malformed line.

### 3.3 Single writer

One writer per `data_dir`. Lock file `$data_dir/.lock` holds pid +
started_at. A lock is **stale** — and taken over — when the pid is
not running, or `/proc/<pid>/stat` starttime does not match the
recorded `started_at` (an OS-recycled pid is not the collector).
Exit code 3 only when pid **and** identity both match a live
process.

### 3.4 Graceful stop and exit codes

- SIGTERM / SIGINT: finish the in-flight batch (flush + CHECKPOINT +
  `max_mb` enforcement), then exit 0. A second signal exits
  immediately with 1.
- `--once`: read one XREAD batch, flush, write CHECKPOINT, enforce
  `max_mb`, exit 0. Equivalent to `--max-reads 1`.
- `--max-reads N`: N XREAD **batches**, then the same clean stop.
- `--max-idle-ms MS`: stop cleanly when no event arrives for MS
  (checked after each read iteration; the stop still flushes, writes
  CHECKPOINT, and enforces `max_mb`). Makes quiet-stream scenarios
  (expiry, empty-stream resume) testable without injecting events.
- The clean-stop path is identical for signals, `--once`,
  `--max-reads`, and `--max-idle-ms`: flush → CHECKPOINT → cap → 0.
- **Exit codes:** 0 clean stop (signal / `--once` / `--max-reads` /
  `--max-idle-ms`); 1 fatal config or IO; 3 lock conflict.

- After every successful flush, enforce `max_mb` (§5.5).
- Lightweight: a few MB RSS, near-zero CPU when the bus is quiet.

```
wfdc                 # follow
wfdc follow
wfdc backfill [--from STREAM_ID] [--to STREAM_ID]
wfdc status          # checkpoint, last flush, redis reachable, bytes used / cap
wfdc status --json   # same, machine-readable (§5.4)
```

### 3.5 backfill

`wfdc backfill [--from STREAM_ID] [--to STREAM_ID]` replays a **chosen
range** of the stream (first install, rebuilt derived tables, or a trimmed
stream you recovered elsewhere) — automatic catch-up is the checkpoint's
job. It writes raw + session rows with the **same writer, decoder and
pairing rules as follow** (§4.1, §5.1, §5.3).

- **Range.** `[--from, --to]` is inclusive on both ends (Redis XRANGE
  semantics). Defaults: `--from 0` (stream start), `--to +` (stream end).
  An inverted range (`--from` after `--to`) or a range with no entries
  writes nothing and exits 0.
- **Dedupe (§3.1) applies.** An entry whose `stream_id` is `<=` the
  **resume point** is skipped, exactly like follow. The resume point is
  `max(durable CHECKPOINT, highest stream id already written to JSONL)`
  — the same startup scan follow performs — so a re-run after a crash
  between appending a batch and writing CHECKPOINT cannot duplicate the
  rows that are already on disk. Re-running a range never duplicates
  rows, and a range that sits entirely at/below the resume point writes
  nothing.
- **Pairing pool is rebuilt from disk.** Before the range is processed,
  the unmatched-start pool (§5.3) is reconstructed from the session rows
  already on disk, so a finish inside the range still pairs with a start
  that was flushed earlier (`<=` resume point) — the same cross-batch
  pool persistence follow has in memory. Skipped entries are never fed
  to the pool a second time.
- **CHECKPOINT is never moved backward.** It advances forward to
  `max(current, last backfilled stream id)` only after everything is on
  disk (§3.1 ordering), so follow-mode checkpoint semantics are
  undisturbed and follow will not re-read the backfilled range. A run
  that wrote nothing leaves CHECKPOINT untouched.
- **Expiry.** Follow evaluates wall-clock expiry (§5.3) on every read
  iteration; backfill has no read iterations, so it evaluates expiry once
  against wall clock at the end of the range — reproducing the session
  state follow would have produced for the same events.
- **Lock and exit codes.** Backfill is a writer: it takes the `.lock`
  (§3.3) and exits **3** when a live collector holds it. Exit **0** on
  success (including empty/inverted ranges), **1** on fatal config/IO
  errors or an invalid `--from`/`--to` (§3.4).

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
2. If `json` or `envelope` is present **and** a valid JSON object: it
   is the envelope. `payload` comes only from that object's `payload`
   key; step 3 does **not** run. Flat known payload keys are still
   kept, under `fields` — they are never used to rebuild the payload
   when `json` is authoritative. Top-level envelope fields missing
   from the JSON object (`id`, `actor`, `action`, `target`,
   `timestamp`, `team`, `project`) are overlaid from the flat map;
   an empty string (`""`) counts as missing for this overlay — the
   `json` envelope stays authoritative.
3. Otherwise (no valid `json`/`envelope` object): if `payload` is a
   valid JSON object string, parse it as the payload (flat known keys
   still go to `fields`). If not, treat known payload keys sitting at
   the top level (`session_id`, `snippet`, `summary`, `task_ref`,
   `handoff`) as the payload.
4. `task_ref` / `handoff` may be JSON strings — parse them when
   valid; a non-JSON plain string is kept as-is under that key (never
   dropped, never replaced with `null`).
5. **Decode failures are never silent.** If `json`/`envelope` is
   present but invalid, the line is still written with all flat
   fields under `fields`, gains `"decode_ok": false`, and the failure
   (stream_id + reason) is logged. The raw dataset keeps the event;
   Lab can see it was not fully decoded.

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

**Timestamp contract.** `timestamp` is parsed as RFC 3339
(`2026-08-30T21:00:00Z` or with a numeric offset). Anything else is
unparsable → `dt=` partitioning and pairing fall back to the Redis
stream-id millisecond clock, which is the **authority for
partitioning and pairing order** (stream ids are monotonic per
stream).

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
- `dt=` is the UTC date of `timestamp` (RFC 3339, §4.2), falling
  back to the Redis stream-id millisecond clock if `timestamp` is
  unparsable.

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
  "fields": {},
  "decode_ok": true
}
```

`fields` is the raw flat map from Redis (strings). `payload` is
whatever decoded as payload (§4.1). `envelope_id` is envelope `id`
when present. `decode_ok` is `true` unless the decoder failed (§4.1
step 5), in which case the line is written with `decode_ok: false`.

### 5.3 Session line

A session is one **agent turn**: one `task.started` paired with a
later `task.finished`.

Hermes `session_id` is a conversation id, not a turn id. Several
starts for the same agent can share one `session_id`. **Do not**
use `(team, actor, session_id)` as the primary pairing key — that
would collapse consecutive turns.

**Pairing (FIFO per agent):**

1. Bucket unmatched starts by `(team, actor)`.
2. A `task.finished` attaches to the **oldest** unmatched start in
   that bucket whose `session_id` is compatible: both empty, or
   equal — a *missing* `session_id` is identical to an empty one.
   "Oldest" means lowest `start_stream_id`.
   - A finish pairs only with a start whose
     `start_stream_id < finish_stream_id`. A finish whose only
     compatible starts have a later or equal stream id is
     `orphan_finish` (this prevents negative `duration_ms`
     structurally — a finish can never close a start that stream
     order says comes after it).
   - If no compatible start is in the pool, the finish is
     `orphan_finish`.
3. A new `task.started` while that agent already has an unmatched
   start marks the previous row `interrupted` and opens a new one.
   **`interrupted` is not terminal:** an interrupted start stays in
   the unmatched pool, and a later compatible finish still pairs
   with it, flipping the row `interrupted` → `completed` via the
   normal upsert (§5.3, "Open rows…"). The transition
   `interrupted` → `completed` is legal and the only way an
   interrupted row changes state. A start that already has a finish
   leaves the pool.

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
  first; still pairable (see rule 3)
- `orphan_finish` — finish with no compatible unmatched start
- `expired` — `open` longer than the expiry window (default 6 h;
  killed container; hooks do not fire on OOM / `docker stop`)

**`orphan_finish` placement.** An orphan finish has no start, so it
lives on the `dt=` of its own finish timestamp (stream-id clock
fallback, §4.2), in the team folder derived from the finish's own
`team` (§5.1). Orphan finishes are **kept** — they are signal (a
start the collector missed, or a hook that fired without its pair).

**Expiry contract.** `expired` is evaluated against **wall clock**
elapsed since `started_at` (envelope timestamp, stream-id fallback),
not stream-id milliseconds. Aging runs on **every read iteration** —
each XREAD round, including empty ones (the ~1 s `BLOCK` timeout
guarantees the loop wakes, so a quiet stream still expires rows).
The window is `--expire-after HOURS` (default 6; env
`WFDC_EXPIRE_HOURS` accepted) — a test knob so the window is
reachable in CI. **`expired` is terminal:** a finish arriving after
its start expired is recorded as `orphan_finish`; an `expired` row
is never resurrected.

Open rows live on the `dt=` of `started_at` and are upserted when
they close, even after midnight. Upsert = rewrite that day's
`sessions.jsonl` via `*.tmp` + atomic rename.

This table exists so Lab does not re-join start/stop on every dump.
It is not a workflow model.

### 5.4 MANIFEST.json

Rewritten each flush. Must **not** store the Redis password.
Write `redis_url` with userinfo stripped (`redis://127.0.0.1:6380`,
never `redis://:secret@…`). Also: plugin version, stream name,
checkpoint, last-flush stream id, event/session counts,
per-`dt=` byte counts (office `raw/`), per-state session counts
(`open` / `interrupted` / `expired` / `completed` /
`orphan_finish`), recent drop-log entries (last 100: when, scope
date-or-today-trim, stream_id/date, bytes freed), discovered
original team strings, `bytes_used`, `max_mb`.

`wfdc status --json` prints the same shape on stdout (one JSON
document, stable key order, no trailing prose) so staging checks
and tests can assert on it. `wfdc status` (human) prints the same
fields as lines.

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
   complete events in **ascending `stream_id` order**, oldest first.
   An event's line is removed from **every** view that contains it —
   office `raw/`, each `teams/*/raw/`, and the `sessions/` row when
   that event is the start or finish edge of a **closed** session.
   `open` session rows are **never** trimmed (an open row near the
   front of today's file stays; the upsert may still complete it
   later). Stop when the tree is under the cap, or the oldest
   remaining event is the only line left in any view that contains
   it (a view never drops to zero lines). Each trimmed event is
   recorded in the drop log (stream_id, when, bytes freed, §5.4).
   Rewrite via `*.tmp` + atomic rename. Never emit a partial JSON
   line.

Checkpoint is **not** moved backward. Dropped rows are a visible
gap for Lab (`status` reports `bytes_used` vs cap and the drop log).
Redis itself is not trimmed.

`0`, a missing key, or a negative value → default 500. 1–15 → 16.

---

## 6. Gaps and honesty

- A hole is a jump in `stream_id` / time, or `status` showing a
  checkpoint older than `XINFO STREAM` last-id. Nothing is
  synthesized to fill it. Hitting `max_mb` also drops the oldest
  on-disk rows; that is intentional, not a collector crash — and it
  is visible in the drop log (§5.4).
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

The at-least-once dedupe (`stream_id <= checkpoint`), the pairing
pool rules (oldest by `start_stream_id`, empty≡missing, stream-id
ordering, `interrupted` stays pairable, expiry terminal), and the
trim order (ascending `stream_id`, event-level across views,
`open` rows protected) are all deterministic: identical stream input
and identical disk state always produce identical output files and
identical drop-log entries.

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

1. Config load + path resolution (incl. env-var base, §2) + Redis
   client + permission enforcement (`wfdc-data/` 0700, files 0600,
   `wfdc.toml` 0600).
2. Follow + checkpoint (atomic `CHECKPOINT.tmp` + rename + fsync,
   §3.1) + `raw/` writer (office-wide and per team) + dedupe +
   startup partial-line repair (§3.2).
3. Session pairing + `sessions.jsonl` (§5.3: pool rules, stream-id
   ordering, `interrupted` → `completed`, `orphan_finish` placement,
   expiry on every read iteration with `--expire-after`).
4. `max_mb` enforcement after flush (§5.5: deterministic event-level
   trim, `open`-row protection, drop log).
5. `MANIFEST.json` (redacted URL, per-`dt=` and per-state counts,
   drop log) + `status` incl. `--json` (§5.4).
6. Deterministic stop contract (§3.4): `--once`, `--max-reads`,
   `--max-idle-ms`, SIGTERM/SIGINT final flush, exit codes.
7. `backfill`.
8. musl binary in `workflow-data-collector/bin/`.

---

## 10. Example config

See `wfdc.toml.example` in this directory (host sidecar default
`redis://127.0.0.1:6380`; containerized runs point at
`redis://shared-memory:6379`).
