# Workflow Data Collector — Specification

**Status:** Draft
**Approach:** spec-driven — this document is the source of truth; implementation follows it.

## 1. Purpose

Collect the workflow events that Agent Office agents emit into Redis and
transform them into structured, analyst-ready datasets. The plugin runs
independently of the factory and does not modify it in any way.

## 2. Motivation

Factory owners want to understand how their factory actually behaves: when
agents start and stop working, which agent did what, and under which command or
team they acted. This plugin turns that raw event stream into clean datasets so
analysts can spot problems and study how the workflow runs — without touching
the factory.

## 3. Input

- **Source:** Redis (the event stream written by Agent Office agents).
- **Events:** agent lifecycle/workflow events, e.g.:
  - agent started working
  - agent stopped working
  - which agent (role)
  - which command / team / factory configuration
- **Consumption model:** the plugin polls Redis continuously (a lightweight
  daemon, ~1 read per second) and processes new events as they appear. It does
  **not** require the factory to push events to it.

  > Open: confirm the exact Redis structure that holds the events
  > (stream vs list vs keys) and the event schema.

## 4. Output

- **Format:** structured datasets for analysts (exact format TBD — CSV / JSONL / Parquet).
  > Open: decide the output format.
- **Location:** a configurable output directory on the host.
  > Open: decide the default path and how it is configured (flag / env / config file).

## 5. Runtime model

- Compiled as a **static `musl` binary** (single file, no system dependencies).
- Runs as a background daemon with a minimal resource footprint.
- Configurable poll interval (default: 1s).

## 6. Non-goals

- Does not modify Agent Office.
- Does not require Agent Office to change how it emits events.
- Does not ship data anywhere — output is written locally.

## 7. Open questions

1. Redis event structure (stream / list / keys) and the exact event schema.
2. Output format (CSV / JSONL / Parquet).
3. Output location and how it is configured.
4. Poll interval default.
5. Binary name and versioning.
