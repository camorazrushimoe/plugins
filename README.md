# Agent Office — Plugins

A collection of standalone plugins for [**Agent Office**](https://github.com/camorazrushimoe/agent-office), the agent factory.
Plugins extend what the factory can do **without modifying the factory itself**.

## What is a plugin?

A plugin is an independent program that runs *next to* the factory, reads the
factory's events, and produces structured output (datasets, metrics, reports).

The factory does not need to know that a plugin exists — it keeps doing its
job, while the plugin quietly collects and transforms data in the background.

Installing a plugin = drop a single compiled binary next to the factory, add
a small config file that points at Redis, and run it. No rebuild of the
factory, no changes to its code.

## Plugin contract

- **Language:** Rust.
- **Distribution:** each plugin is compiled as a **static `musl` binary** — a
  single, self-contained executable with no dependencies on system libraries.
  It runs on any Linux host or container out of the box.
- **Config:** a TOML file next to the binary (Redis URL / stream / data dir).
  Port and host vary per factory; they are not compiled in.
- **Source:** source code is kept in this repository for transparency and
  rebuilds, but the *install unit* is the prebuilt binary.
- **Runtime:** plugins are lightweight by design (a few MB of memory, near-zero
  CPU when idle), so they can run continuously alongside the factory.

## Development approach

Plugins are developed **spec-driven**: the specification (`SPEC.md`) is written
first and is the source of truth. Implementation follows the spec.

## Ship unit (build & verify)

The install unit is a **reproducible static musl binary** (SPEC §9.8).
`workflow-data-collector/build.sh` builds it with a **pinned toolchain**
(`rustup 1.98.0` + `x86_64-unknown-linux-musl` — no `latest` drift) and
records the binary's sha256 in `workflow-data-collector/bin/SHA256SUMS`:

```bash
./workflow-data-collector/build.sh
./workflow-data-collector/verify-ship-unit.sh          # static / sha256 / version checks
./workflow-data-collector/verify-ship-unit.sh --repro  # + two-clean-build reproducibility
```

Two clean builds of the same commit in the pinned toolchain produce
byte-identical binaries (identical sha256). `workflow-data-collector/bin/wfdc`
is the committed install unit; the version it reports is the same version
`MANIFEST.json` carries as `plugin_version` (SPEC §5.4).

## Layout

```
plugins/
├── workflow-data-collector/   # turns agent workflow events into analyst-ready datasets
│   ├── SPEC.md                # specification (source of truth)
│   └── wfdc.toml.example      # config dropped next to the binary
└── ...
```

Each plugin folder will contain its source (`src/`) and prebuilt binaries
(`bin/`) as they are built.

## Plugins

| Plugin | What it does | Status |
|--------|--------------|--------|
| [workflow-data-collector](workflow-data-collector/) | Reads the factory Redis stream and writes a raw JSONL dataset plus paired agent sessions. Lab analyses the files. | Specification v0.3.0 |
