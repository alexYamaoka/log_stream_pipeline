# Project Overview

## What we're building

A **real-time distributed data pipeline** that ingests a high-throughput stream of
application logs, buffers them safely, processes them, and stores them in a way
that's searchable — all running locally on a single laptop via Docker.

The folder is named `log_streams` because we chose the **system-logs / infrastructure**
flavor of the project (see [design-decisions.md](design-decisions.md), "Path A vs Path B").

## The data flow

```
[Mock log generator]    [Buffer / shock absorber]   [Worker]              [Searchable store]
   Java Producer  ─────►   Apache Kafka   ─────►   Python Consumer  ─────►   OpenSearch
   (Spring Boot)          (holds messages)         (batch + index)          (stores + serves search)
```

| Stage | Tech | Job |
|-------|------|-----|
| **Ingestor** | Java / Spring Boot | Generate realistic fake logs and push them into Kafka fast |
| **Broker** | Apache Kafka | Hold the stream safely so nothing drops if the worker is slow |
| **Processor** | Python | Pull text, batch it, index it (optionally embed it) |
| **Storage** | OpenSearch | Store logs so you can full-text + filter search them |

## What this project is really about

The headline isn't "AI." It's **distributed systems**. The pipeline is designed to
*demonstrate* these production concepts on a laptop:

- **Decoupling** — each component is its own process/container, talking only over the
  network. Any one can crash without taking the others down.
- **Fault tolerance** — Kafka persists messages to disk, so a crashed worker loses nothing.
- **Backpressure** — if the slow part (indexing) bogs down, the backlog safely piles up
  in Kafka instead of overwhelming anything.
- **Horizontal scaling** — run multiple identical workers and Kafka auto-balances the load.
- **Dead-letter queue** — bad messages are quarantined, not allowed to crash the pipeline.

## Why two languages?

It mirrors how real companies actually build these systems:
- **Java** for high-throughput, low-latency ingestion.
- **Python** for the data/ML processing.

This polyglot split is itself a portfolio talking point, and it sets up the
"asymmetric scaling" story — ingestion and processing have very different
performance profiles, so being able to scale them independently is the point.

## Who it's for (the resume angle)

- **Infra / Platform engineering teams** ← this project, as built (Path A).
  Emphasizes high-throughput streaming, fault tolerance, and scaling.
- An alternate "Path B" (wiki documents + vector DB) would target **AI Application**
  teams instead. We chose Path A; see [design-decisions.md](design-decisions.md).

## Important honest caveats

- **We are *simulating* a distributed cluster, not running a real one.** Everything
  is one node each (1 Kafka broker, 1 OpenSearch node, 1 partition). The *code and
  architecture* are identical to production; the scale is not. Be precise about this
  distinction when describing the project — claiming a "distributed Raft cluster" on a
  laptop with one node is overselling it.
- **Nothing has been run/verified end-to-end yet** as of writing these docs. The code
  is scaffolding. First real compile happens when IntelliJ imports the Java project.
- **The data is simulated.** We generate fake logs rather than ingesting real ones —
  this is intentional and good (full control over throughput for stress tests).
