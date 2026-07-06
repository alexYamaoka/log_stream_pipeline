# LogStream Pipeline — The Project

Consolidated project documentation: what we built, why, and what's next.

**Companion docs:** `DISTRIBUTED-SYSTEMS.md` (how the tech works, deep dives) ·
`SYSTEM-DESIGN-NOTES.md` (interview prep). Cross-references to other doc names now
refer to sections within these three files.

## Contents
1. **Overview** — what we're building, architecture, caveats
2. **Design Decisions & Rationale** — every choice + why + trade-offs
3. **Open Questions & Next Steps**

---

# Project Overview

## What we're building

A **real-time distributed data pipeline** that ingests a high-throughput stream of
application logs, buffers them safely, processes them, and stores them in a way
that's searchable — all running locally on a single laptop via Docker.

The folder is named `log_streams`. The data is simulated application logs.

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
| **Processor** | Python | Pull logs, batch them, index them |
| **Storage** | OpenSearch | Store logs so you can full-text + filter search them |

## What this project is really about

The point is **distributed systems**. The pipeline demonstrates these production
concepts on a laptop:

- **Decoupling** — each component is its own process/container, talking only over the
  network. Any one can crash without taking the others down.
- **Fault tolerance** — Kafka persists messages to disk, so a crashed worker loses nothing.
- **Backpressure** — if the slow part (indexing) bogs down, the backlog safely piles up
  in Kafka instead of overwhelming anything.
- **Horizontal scaling** — run multiple identical workers and Kafka auto-balances the load.
- **Dead-letter queue** — bad messages are quarantined, not allowed to crash the pipeline.

## Why two languages?

- **Java** for high-throughput, low-latency ingestion.
- **Python** for the consuming and indexing side.

The split also enables asymmetric scaling — ingestion and processing have different
performance profiles, so they can be scaled independently.

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


---

# Design Decisions & Rationale

This is the heart of the docs: every meaningful decision, *why* we made it, what
makes it **more robust**, and the **trade-offs**. Each entry is written so you can
defend the choice in an interview.

---

## 1. Run everything locally with Docker (simulate distribution)

**Decision:** Use Docker Compose to run Kafka, OpenSearch, etc. as separate
containers on one laptop.

**Why:** Lets us simulate a multi-node distributed environment with zero cloud
cost. Each container has its own memory/process space and talks only over the
network — exactly like production, just smaller.

**What makes it robust:** Because components only communicate over TCP/IP, the
*same code* would work if every piece moved to a different server. Nothing relies
on shared memory.

**Trade-off / caveat:** It's a *simulation*, not a real cluster. One broker, one
OpenSearch node, one partition. Don't oversell it as a true distributed cluster.

---

## 2. Two languages: Java (ingest) + Python (process)

**Decision:** Java Spring Boot produces; Python consumes.

**Why:** Mirrors industry practice. Java excels at high-throughput, low-latency
ingestion; Python is the lingua franca of data/ML processing.

**What makes it robust:** The split enables **independent (asymmetric) scaling** —
ingestion and processing have very different performance profiles, and decoupling
them means you can scale each separately.

**Trade-off:** More moving parts and two toolchains to manage vs. a single-language
monolith. Worth it here because demonstrating the polyglot, decoupled architecture
*is* the goal.

---

## 3. Kafka as the message broker (the "shock absorber")

**Decision:** Put Apache Kafka between the producer and consumer.

**Why:** Decouples sender and receiver so they run at independent speeds. Kafka
persists messages to disk.

**What makes it robust (4 concrete wins):**
- **Spatial decoupling** — producer and consumer never call each other directly;
  one crashing can't crash the other.
- **Fault tolerance** — if the worker dies, Java keeps producing into Kafka; Kafka
  holds the data on disk; the worker resumes from its bookmark with zero loss.
- **Backpressure** — if processing slows, the backlog piles up safely in Kafka
  instead of overwhelming downstream.
- **Asynchronous buffering** — smooths out traffic spikes; the producer can finish
  a 10k burst instantly while the consumer drains it steadily.

**Trade-off:** Kafka adds operational weight (it's a JVM service with its own
storage and config). For a tiny app this is overkill; for demonstrating distributed
systems, it's the centerpiece.

---

## 4. OpenSearch as the store

**Decision:** Use **OpenSearch** (a full-text + structured search engine) as the store.

**Why:** Logs are **structured and keyword-heavy**. The questions you ask logs are mostly:
- "All `ERROR` logs from `payment-api` in the last hour" → **structured filter**
- "Logs containing `NullPointerException`" → **keyword / full-text**

Full-text + filtering handles the large majority of real log queries. OpenSearch is the
industry standard for log pipelines (the "ELK / observability stack").

**What makes it robust:**
- Self-contained storage (Lucene-backed) — no separate database needed.
- It's the search engine *and* the datastore in one.

**Trade-off:** OpenSearch is JVM-based and relatively heavy (~1–1.5 GB heap). Fine on a
16 GB machine; something to tune on 8 GB.

---

## 8. Spring Boot + spring-kafka instead of the raw Kafka client

**Decision:** The Java producer is a Spring Boot app using `spring-kafka`'s
`KafkaTemplate`, not the hand-rolled `KafkaProducer`.

**Why:** Idiomatic, runs cleanly in IntelliJ (click ▶), config lives in
`application.yml`, and dependency injection removes lifecycle boilerplate.

**What makes it robust / better:**
- Configuration is externalized (`application.yml`) — change Kafka settings without
  touching code.
- Spring manages the producer's lifecycle (creation, shutdown) for you.
- `KafkaTemplate` is thread-safe and reusable; you just call `.send()`.

**Trade-off:** Pulls in the Spring framework (heavier dependency, "magic" that's
opaque until you understand DI). Worth it for the developer experience and because
Spring Boot is what most Java shops actually use.

---

## 9. JDK 21 (not the system's JDK 26)

**Decision:** Build/run the Java app on **JDK 21** (LTS), configured inside IntelliJ.

**Why:** Spring Boot 3.3 officially supports **JDK 17–22**. The machine has JDK 26
installed, which is newer than Spring Boot 3.3's tested range and can produce
warnings or edge-case breakage.

**What makes it robust:** JDK 21 is an LTS release fully supported by the framework —
the safe, boring, correct choice. IntelliJ can download and manage it without a
system install.

**Trade-off:** None meaningful. (The `pom.xml` targets Java 17 bytecode, which any
JDK ≥17 compiles happily.)

---

## 10. 16 GB "comfortable" resource config

**Decision:** Cap OpenSearch heap at 1 GB; run Kafka + OpenSearch + Dashboards +
both apps simultaneously.

**Why:** On 16 GB there's headroom for everything at once without heap-budgeting
gymnastics. (On 8 GB we'd tighten heaps and stage startup.)

**What makes it robust:** Explicitly capping the JVM heap (`OPENSEARCH_JAVA_OPTS=-Xms1g -Xmx1g`)
keeps OpenSearch from grabbing far more RAM than it needs — predictable footprint.

**Trade-off:** None at 16 GB. Worth revisiting if the workload grows.

---

## 11. Kafka in KRaft mode, single node (no ZooKeeper)

**Decision:** One Kafka container plays both `broker` and `controller` roles using
**KRaft** (Kafka Raft), the modern built-in replacement for ZooKeeper.

**Why:** Simpler — one container instead of two (Kafka + ZooKeeper). KRaft is the
current standard direction for Kafka.

**What makes it robust:** Fewer moving parts to fail; modern, supported config.

**Trade-off:** Single node = no real replication/HA. Fine for a laptop simulation.

---

## 12. Manual offset commits → at-least-once delivery

**Decision:** Python consumer uses `enable_auto_commit=False` and commits the offset
**only after** a batch is successfully indexed.

**Why:** The default (auto-commit on a timer) can mark messages "done" *before*
they're saved — a crash in that window loses data.

**What makes it robust:** Save-to-OpenSearch-first, then commit. If the worker dies
mid-batch, the bookmark hasn't moved, so Kafka re-delivers those messages on
restart. This is **at-least-once delivery** — never lose a message (may rarely
process one twice).

**Trade-off:** Possible duplicate processing. Made harmless via **idempotency**:
we use the log's `id` as the OpenSearch document `_id`, so re-saving just overwrites
identical data (decision #14).

---

## 13. Batching (don't process one message at a time)

**Decision:** Collect up to ~200 messages, then send them to OpenSearch in a single
`helpers.bulk()` request. Java side batches too (`linger.ms=20`, `batch.size`).

**Why:** Each network round-trip has fixed overhead. Bundling many items per trip
is dramatically more efficient than one-at-a-time.

**What makes it robust:** Higher throughput, fewer connections, less overhead — the
#1 throughput lever in data pipelines.

**Trade-off:** Slightly higher latency per message (you wait to fill a batch) and
more memory per batch. Negligible here; tunable via `BATCH_SIZE`.

---

## 14. Idempotency via log `id` as the document `_id`

**Decision:** Use each log's own `id` (a UUID) as its OpenSearch `_id`.

**Why:** Combined with at-least-once delivery, re-saving the same log just overwrites
the existing document with identical content.

**What makes it robust:** Makes the pipeline **idempotent** — reprocessing is safe,
no duplicate documents. This is what makes "at-least-once" acceptable in practice.

**Trade-off:** None meaningful for this use case.

---

## 15. Dead-letter queue for bad messages

**Decision:** If a message can't be parsed/processed, route it to a separate Kafka
topic `logs-dlq` instead of crashing the worker.

**Why:** One corrupt message shouldn't halt the entire pipeline.

**What makes it robust:** Failures are *isolated and preserved* — the worker keeps
going, and you can inspect/replay `logs-dlq` later. Standard production hygiene.

**Trade-off:** You must remember to monitor/drain the DLQ; messages there aren't
acted on automatically.

---

## 16. Consumer groups for horizontal scaling

**Decision:** The Python worker joins a named consumer group (`log-indexers`).

**Why:** Kafka guarantees each message goes to exactly one member of a group, and
auto-balances partitions across members.

**What makes it robust:** To scale, just launch more copies of `consumer.py` with the
same `group_id` — Kafka distributes the load and rebalances on crash/join. **Zero
coordination code.**

**Trade-off / current limitation:** We have only **1 partition**, so only one worker
gets work at a time. To actually demonstrate parallelism, create `raw-logs` with
multiple partitions (e.g., 3). See open-questions.md.

---

## 17. Workload modes: steady vs. spike

**Decision:** The producer supports `app.mode=steady` (paced drip) and
`app.mode=spike` (blast N logs as fast as possible).

**Why:** Steady simulates normal operation; spike is the **stress test** that
showcases the architecture — Java finishes instantly, Kafka's queue spikes, the
worker drains the backlog without crashing.

**What makes it robust:** Lets you actually *observe* backpressure and buffering
under load, not just claim them.

**Trade-off:** None — it's a testing affordance.

---

## 18. Random UUIDs for log IDs (not sequential counters)

**Decision:** Each log's `id` is a random UUID (`UUID.randomUUID()` in
`LogSimulator`), not an incrementing counter.

**Why:** In a distributed system, IDs should be generatable by any node
independently, with no shared counter to coordinate and no collisions.

**What makes it robust:**
- **No shared state / no coordination.** A sequential counter would need a single
  source of truth; multiple producers (or restarts) would race or duplicate it.
- **Restart-safe.** The producer holds no state — stop and restart it and it keeps
  minting fresh unique IDs. A counter would reset to 1 on restart (unless persisted)
  and overwrite the previous run's documents in OpenSearch.
- **Collision-free across runs and nodes.** ~122 bits of randomness make duplicates
  effectively impossible, so two independent producers never clash.
- **Pairs with idempotency (#14).** The id travels with the message through Kafka and
  becomes the OpenSearch `_id`. If Kafka re-delivers the *same* message (at-least-once),
  the same id overwrites harmlessly; genuinely new logs always get distinct ids.

**Trade-off:** UUIDs aren't human-readable or time-ordered, and are larger than an
integer. For log events that's fine — uniqueness and zero-coordination matter more
than ordering (and the `timestamp` field already gives us time order).


---

# Open Questions & Next Steps

Things not yet done, decisions still open, and ideas to level up the project.
Update this as we go.

## Not yet verified

- [ ] **Nothing has been run end-to-end.** The code is scaffolding. First Java compile
      happens when IntelliJ imports the project.
- [ ] Confirm the Docker stack comes up cleanly (`docker compose up -d`) and OpenSearch
      responds (`curl localhost:9200`).
- [ ] Push real messages through and confirm the count climbs
      (`curl localhost:9200/logs/_count`).
- [ ] Confirm IntelliJ runs `ProducerApplication` on JDK 21 without
      `UnsupportedClassVersion`-type errors.

## Known limitations to address

- [ ] **Single partition.** `raw-logs` has 1 partition, so multiple workers can't run
      in parallel. To demonstrate horizontal scaling, create the topic with ~3
      partitions and run 2–3 `consumer.py` instances with the same `GROUP_ID`.
- [ ] **No replication / HA.** Single Kafka broker and single OpenSearch node. Expected
      for a laptop simulation — just be precise that it's a simulation, not a real
      distributed cluster.

## Enhancement ideas

- [ ] **Dashboards visualizations.** Build a few OpenSearch Dashboards panels (errors
      over time, errors by service).
- [ ] **Metrics/observability.** Expose consumer lag or throughput numbers to make the
      backpressure story visible.
- [ ] **Replay tooling for the DLQ.** A small script to inspect and re-submit
      `logs-dlq` messages after fixing the cause.
- [ ] **Schema / contract.** Consider a typed schema (e.g., JSON Schema or Avro +
      Schema Registry) for the log format to show data-contract awareness.
- [ ] **Kafka producer idempotence (defense in depth).** Set `acks=all` +
      `enable.idempotence=true` so producer retries can't write duplicates into the log.
      Optional — our sink-side idempotency (deterministic `_id`) already covers it. See
      idempotency-and-reliability.md §8.
- [ ] **Harden the consumer's bulk/commit.** Wrap `helpers.bulk` + `consumer.commit` in
      try/except with retry + backoff so a transient OpenSearch error pauses-and-retries
      instead of crashing the worker. See
      idempotency-and-reliability.md §6b.
- [ ] **Containerize the apps too.** Add the Java and Python services to
      docker-compose so the whole thing is one `up`.

## Decisions still open

- [ ] Final partition count and whether to demonstrate a multi-broker Kafka cluster.
      Walkthrough + production context in
      multi-broker-setup.md.

## Housekeeping

- [ ] Project is **not a git repo yet.** Run `git init` + first commit when ready.
