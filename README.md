# Agent Office — Plugins

A collection of standalone plugins for **Agent Office**, the agent factory.
Plugins extend what the factory can do **without modifying the factory itself**.

## What is a plugin?

A plugin is an independent program that runs *next to* the factory, reads the
factory's events, and produces structured output (datasets, metrics, reports).

The factory does not need to know that a plugin exists — it keeps doing its
job, while the plugin quietly collects and transforms data in the background.

Installing a plugin = drop a single compiled binary next to the factory and
run it. No rebuild of the factory, no changes to its code.

## Plugin contract

- **Language:** Rust.
- **Distribution:** each plugin is compiled as a **static `musl` binary** — a
  single, self-contained executable with no dependencies on system libraries.
  It runs on any Linux host or container out of the box.
- **Source:** source code is kept in this repository for transparency and
  rebuilds, but the *install unit* is the prebuilt binary.
- **Runtime:** plugins are lightweight by design (a few MB of memory, near-zero
  CPU when idle), so they can run continuously alongside the factory.

## Development approach

Plugins are developed **spec-driven**: the specification (`SPEC.md`) is written
first and is the source of truth. Implementation follows the spec.

## Layout

```
plugins/
├── workflow-data-collector/   # turns agent workflow events into analyst-ready datasets
│   └── SPEC.md                # specification (source of truth)
└── ...
```

Each plugin folder will contain its source (`src/`) and prebuilt binaries
(`bin/`) as they are built.

## Plugins

| Plugin | What it does | Status |
|--------|--------------|--------|
| [workflow-data-collector](workflow-data-collector/) | Collects agent lifecycle/workflow events from Redis and transforms them into structured datasets for analysts | Specification |
