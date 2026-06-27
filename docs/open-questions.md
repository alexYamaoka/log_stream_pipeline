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

## Enhancement ideas (portfolio level-ups)

- [ ] **Phase 2: semantic search.** Turn on `USE_EMBEDDINGS=true`, pull `all-minilm`
      via Ollama, and demonstrate **hybrid search** (full-text + kNN) — "find logs
      similar to this incident."
- [ ] **Dashboards visualizations.** Build a few OpenSearch Dashboards panels (errors
      over time, errors by service) for portfolio screenshots.
- [ ] **Metrics/observability.** Expose consumer lag or throughput numbers to make the
      backpressure story visible.
- [ ] **Replay tooling for the DLQ.** A small script to inspect and re-submit
      `logs-dlq` messages after fixing the cause.
- [ ] **Schema / contract.** Consider a typed schema (e.g., JSON Schema or Avro +
      Schema Registry) for the log format to show data-contract awareness.
- [ ] **Kafka producer idempotence (defense in depth).** Set `acks=all` +
      `enable.idempotence=true` so producer retries can't write duplicates into the log.
      Optional — our sink-side idempotency (deterministic `_id`) already covers it. See
      [idempotency-and-reliability.md](idempotency-and-reliability.md) §8.
- [ ] **Harden the consumer's bulk/commit.** Wrap `helpers.bulk` + `consumer.commit` in
      try/except with retry + backoff so a transient OpenSearch error pauses-and-retries
      instead of crashing the worker. See
      [idempotency-and-reliability.md](idempotency-and-reliability.md) §6b.
- [ ] **Containerize the apps too.** Add the Java and Python services to
      docker-compose so the whole thing is one `up`.

## Decisions still open

- [ ] Whether to ever add Path B (wiki/RAG) as a second showcase, or keep this focused
      on the infra story.
- [ ] Final partition count and whether to demonstrate a multi-broker Kafka cluster.
      Walkthrough + production context in
      [multi-broker-setup.md](multi-broker-setup.md).

## Housekeeping

- [ ] Project is **not a git repo yet.** Run `git init` + first commit when ready.
