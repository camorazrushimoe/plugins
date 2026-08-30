# Workflow Data Collector — Specification

**Plugin:** `workflow-data-collector`
**Binary:** `wfdc`
**Status:** Draft v0.2
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
verdicts, and chain mining stay in Lab. They can be added to a later
plugin revision after the first dumps have been looked at.

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
# wfdc.toml — loaded from the binary's directory, then cwd, then --config
redis_url = "redis://127.0.0.1:6380"
stream    = "office:events"
data_dir  = "./wfdc-data"
```

`redis_url` is how a factory on a non-default port is targeted.
Host, port, password, db index all live in the URL.

CLI overrides the file when passed:

```
./wfdc                  # follow, using wfdc.toml
./wfdc --config /path/wfdc.toml
./wfdc --redis redis://127.0.0.1:6379 --stream office:events
```

Env vars (`WFDC_REDIS_URL`, `WFDC_STREAM`, `WFDC_DATA_DIR`,
`WFDC_CONFIG`) also work. Precedence: CLI > env > config file >
built-in defaults (`redis://127.0.0.1:6380`, `office:events`,
`./wfdc-data`).

---

## 3. Runtime

- Default command is **follow**: block on the stream, write new
  entries as they appear. A one-second block timeout is enough;
  there is no busy-loop.
- If Redis is down or the stream does not exist: log, wait, retry
  with backoff. Do not exit. Do not invent events. The gap is
  visible later as a jump in `stream_id` / timestamps — Lab treats
  that as missing data.
- Checkpoint the last successfully flushed Redis stream id in
  `$data_dir/CHECKPOINT`. After a restart, resume from that id.
  Events that arrived while the process was dead are not backfilled
  automatically in v1 (the stream may still hold them; `wfdc backfill`
  exists for a manual catch-up).
- One writer per `data_dir` (pid lock file). Lightweight: a few MB
  RSS, near-zero CPU when the bus is quiet.

```
wfdc                 # follow
wfdc follow
wfdc backfill [--from STREAM_ID] [--to STREAM_ID]
wfdc status          # checkpoint, last flush, redis reachable?
```

---

## 4. What is read

Source of truth: Redis **STREAM** at `stream` from the config
(Office default: `office:events`). Pub/sub is ignored.

Every stream entry is stored raw. v1 session pairing only looks at:

- `task.started`  — agent:start hook
- `task.finished` — agent:end hook

Any other `action` is kept in the raw log and skipped by the
assembler. Unknown payload fields are kept as-is.

Envelope (already published by the factory, see
`agent-office/bus/action-schema.json`):

| Field | Use |
|-------|-----|
| Redis stream id | order + checkpoint + gap detection |
| `id` | envelope uuid |
| `timestamp` | event time (UTC) |
| `actor` | agent id |
| `action` | event type |
| `target` | as published |
| `team` | instance id (`dev-1`, `lab-1`, `office`, …) |
| `project` | if present |
| `payload` | opaque JSON; known keys below |

Known payload keys from the activity hooks: `session_id`, `snippet`,
`summary`, `task_ref` (`issues` / `prs` / `linear`), `handoff`
(finish only). Missing keys stay null. They are not guessed.

The plugin never asks the factory to add fields.

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
    <team>/
      raw/dt=YYYY-MM-DD/events.jsonl
      sessions/dt=YYYY-MM-DD/sessions.jsonl
```

- `raw/` is the dataset. Append-only. One JSON object per line.
- `teams/<team>/` is the same events split by `team` so Lab can
  take one folder for one factory instance. Empty / missing `team`
  goes to `_unknown` (or `_office` when the actor is an Office role).
- `sessions/` is the only derived table in v1.
- `dt=` is the UTC date of `timestamp`, falling back to the Redis
  stream-id clock if the timestamp is unparsable.

`MANIFEST.json` records plugin version, `redis_url`, `stream`,
checkpoint, event/session counts, discovered teams. Rewritten on
each flush.

### 5.1 Raw event line

```json
{
  "stream_id": "1725062400000-0",
  "envelope_id": "…",
  "ts": "2026-08-30T21:00:00Z",
  "actor": "developer",
  "action": "task.started",
  "target": "developer",
  "team": "dev-1",
  "project": null,
  "payload": { }
}
```

`payload` is the factory object unchanged.

### 5.2 Session line

A session is one agent turn: a `task.started` paired with a later
`task.finished` on the same correlation key.

Key: `(team, actor, session_id)` when `session_id` is present.
Otherwise FIFO: match a finish to the oldest unmatched start of
that `(team, actor)`.

| Field | Meaning |
|-------|---------|
| `session_pk` | stable hash of the start identity |
| `team`, `actor`, `session_id` | |
| `start_stream_id`, `finish_stream_id` | Redis ids; finish may be null |
| `started_at`, `finished_at` | |
| `duration_ms` | null while open |
| `state` | `completed` \| `open` \| `interrupted` \| `orphan_finish` \| `expired` |
| `snippet_in`, `snippet_out` | from start / finish payload |
| `issues`, `prs`, `linear` | union of start+finish refs |
| `handoff` | from finish |
| `project` | if present on either envelope |

States:

- `completed` — start and finish, in order
- `open` — start seen, no finish yet
- `interrupted` — a new start arrived for the same agent while the
  previous session was still open
- `orphan_finish` — finish with no matching start (process started
  mid-stream, or the start was lost)
- `expired` — `open` longer than 6 hours (likely a killed container;
  hooks do not fire on OOM / `docker stop`)

Open rows live on the `dt=` of `started_at` and are upserted when
they close, even after midnight.

This table exists so Lab does not have to re-join start/stop on
every dump. It is not a workflow model.

---

## 6. Gaps and honesty

- A Redis outage or a stopped collector leaves a hole in `raw/`.
  Nothing is synthesized to fill it. `status` and the checkpoint
  make the hole discoverable.
- Blockers are not labelled. The factory hooks do not emit
  `blocked`, and the collector will not regex them out of snippets.
- Snippets are whatever the factory already put on the bus
  (first 200 characters). Stored as-is.
- Model name, tokens, full prompts are not on the bus today.
  Out of scope until the factory itself publishes them — this
  plugin will not require that change.

---

## 7. Determinism

Same stream range + same pairing rules → same `session_pk` values.
Raw files are never rewritten; only appended. Derived session files
may rewrite the current day via `*.tmp` + atomic rename.

---

## 8. Non-goals (v1)

- Changing Agent Office or any team template
- Pushing data off the box
- Dashboards
- CSV / Parquet
- LLM
- Reconstructing multi-agent chains (do this in Lab on top of
  sessions + `handoff`; promote into the plugin later if it earns
  its keep)
- Creating the Redis stream if it is missing

---

## 9. Implementation order

1. Config load (`wfdc.toml` / env / flags) + Redis client.
2. Follow + checkpoint + `raw/` writer (office-wide and per team).
3. Session pairing + `sessions.jsonl`.
4. `MANIFEST.json` + `status`.
5. `backfill`.
6. musl binary in `workflow-data-collector/bin/`.

---

## 10. Example config

See `wfdc.toml.example` in this directory.
