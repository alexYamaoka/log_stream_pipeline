# Glossary

Quick-reference vocabulary for the project. Fuller explanations live in
[kafka-explained.md](kafka-explained.md) and [consumer-explained.md](consumer-explained.md).

## Kafka

- **Broker** — the Kafka server itself (our `kafka` Docker container).
- **Cluster** — multiple brokers working together; partitions (and their replicas) are
  spread across them. We run a single-broker "cluster."
- **Replication factor (RF)** — how many copies of each partition exist, on different
  brokers. RF=1 (ours) = no redundancy.
- **Leader / follower** — per partition, the **leader** replica handles all reads/writes;
  **followers** passively copy it. If the leader's broker dies, a follower is promoted.
- **In-sync replica (ISR)** — a follower fully caught up with the leader; `acks=all` waits
  for the ISR. See [kafka-reference.md](kafka-reference.md) §2b.
- **Controller** — in KRaft, the node(s) managing cluster metadata (the "brain"). Production
  keeps a small fixed set (3 or 5), separate from brokers, so brokers scale freely.
- **Partition reassignment** — the controlled operation (`kafka-reassign-partitions.sh`)
  that moves partitions/replicas across brokers — e.g., onto a newly added broker. Data
  doesn't move itself. See [multi-broker-setup.md](multi-broker-setup.md).
- **Cruise Control** — open-source tool that automates partition rebalancing/reassignment,
  giving the "one-click scale" experience on self-managed clusters.
- **Topic** — a named stream/"belt" of messages (e.g., `raw-logs`, `logs-dlq`).
- **Partition** — a topic split into parallel lanes; the unit of parallelism. We
  currently have 1.
- **Producer** — a program that writes messages to a topic (our Java app).
- **Consumer** — a program that reads messages from a topic (our Python app).
- **Consumer group** — a team of consumers sharing a `group_id`; Kafka delivers each
  message to exactly one member and auto-balances partitions across them.
- **Rebalance** — Kafka re-dealing partitions among a consumer group's members when one
  joins, leaves, or crashes. Automatic; can cause a brief pause and at-least-once
  re-processing. See [scaling-and-backpressure.md](scaling-and-backpressure.md).
- **Consumer lag** — `log-end-offset − current-offset` = how many messages a consumer
  still hasn't processed. The key health/backpressure metric.
- **Offset** — a message's sequential position in a partition (#0, #1, …); a
  consumer's "bookmark" of how far it has processed.
- **Commit (offset commit)** — saving the bookmark. We do it *manually, after* a batch
  is indexed (at-least-once delivery).
- **KRaft** — Kafka's modern built-in cluster-coordination mechanism; replaces the old
  ZooKeeper dependency.
- **Serializer / Deserializer** — translators between your data and the raw **bytes**
  Kafka moves. Producer serializes (string→bytes); consumer deserializes (bytes→string).
- **KafkaTemplate** — Spring's high-level wrapper around the raw `KafkaProducer`; you
  just call `.send()`.

## Delivery & reliability

- **Decoupling** — producer and consumer never call each other directly; they only
  share Kafka. One crashing can't crash the other.
- **Backpressure** — when a downstream step is slow, work safely queues upstream
  (in Kafka) instead of overwhelming anything. In our pipeline it's automatic: the
  synchronous index-then-commit loop + Kafka's pull model. See
  [scaling-and-backpressure.md](scaling-and-backpressure.md).
- **At-least-once delivery** — every message is processed at least once; may rarely be
  processed twice (never lost). Result of committing offsets *after* saving.
- **At-most-once delivery** — commit *before* processing; never duplicates but can lose
  messages on crash. (We do NOT use this.)
- **Exactly-once** — each message effected exactly once; true end-to-end version needs
  distributed transactions + a transactional sink. Hard/expensive.
- **Effectively-once** — the pragmatic equivalent: at-least-once delivery + idempotent
  processing. **This is what our pipeline does.**
- **Idempotency** — reprocessing the same message has no extra effect. We get it by using
  the log's `id` as the OpenSearch document `_id`, so re-saves overwrite. The pillar that
  neutralizes the duplicates that retries/at-least-once create. See
  [idempotency-and-reliability.md](idempotency-and-reliability.md).
- **Idempotent producer** — Kafka's `enable.idempotence=true` (needs `acks=all`); dedupes
  the producer's *own* retries into the log. Off in our config (`acks=1`); our sink-side
  idempotency covers it anyway.
- **Dead-letter queue (DLQ)** — a separate topic (`logs-dlq`) where unprocessable
  messages are quarantined so they don't crash the pipeline.

## OpenSearch

- **OpenSearch** — a Lucene-based search engine + datastore; the open-source fork of
  Elasticsearch. Stores our logs and serves search. It IS the storage (no separate DB).
- **Elasticsearch** — the original project OpenSearch forked from (2021); ~95% the same.
- **Index** — roughly a "table" in OpenSearch (our `logs` index).
- **Mapping** — the index's schema (field names + types).
- **`keyword` field** — stored as one exact value; used for filters and aggregations
  (exact match).
- **`text` field** — analyzed into words and indexed for full-text search.
- **knn_vector** — a field type holding an embedding vector for semantic/kNN search
  (our optional phase-2 feature).
- **Bulk request** — indexing many documents in one network call (`helpers.bulk`).

## AI / search concepts

- **Vector embedding** — a numeric vector representing the *meaning* of text; lets you
  find semantically similar items. Optional here (phase 2).
- **Full-text search** — keyword/word-based search (BM25). Our backbone for logs.
- **Hybrid search** — combining full-text + vector search; the best-of-both approach.
- **RAG (Retrieval-Augmented Generation)** — retrieve relevant docs, feed them to an
  LLM to answer questions. The "Path B" use case we did *not* center on.
- **Ollama** — runs small models locally; we'd use it for embeddings in phase 2
  (e.g., `all-minilm`, 384-dim).

## Java / Spring

- **Spring Boot** — Java framework that auto-configures components from `application.yml`
  and runs with one click.
- **Dependency injection (DI)** — Spring constructs objects (like `KafkaTemplate`) and
  hands them to your classes via the constructor, instead of you `new`-ing them.
- **`@SpringBootApplication`** — marks the main entry-point class.
- **`ApplicationRunner`** — a Spring hook that runs once on startup (our producer loop).
- **JDK / LTS** — Java Development Kit; LTS = Long-Term Support release (we use JDK 21).
