# LogStream Pipeline — Documentation & Learning Log

This folder captures everything we've figured out while designing this project:
what we're building, why we made each decision, what makes those decisions more
robust, and ground-up explanations of the moving parts.

It doubles as **interview prep** — every design choice here has a "why" you can
defend out loud.

## Suggested reading order

1. [overview.md](overview.md) — what we're building and the big picture
2. [kafka-explained.md](kafka-explained.md) — the buffer, conceptually
3. [consumer-explained.md](consumer-explained.md) — the worker + how logs reach OpenSearch
4. [kafka-reference.md](kafka-reference.md) — go deeper: Kafka features, CLI/config syntax, how our code uses it
5. [opensearch-reference.md](opensearch-reference.md) — go deeper: OpenSearch features, Query DSL syntax, how our code uses it
6. [scaling-and-backpressure.md](scaling-and-backpressure.md) — runtime behavior under load: backpressure, rebalancing, adding capacity
7. [idempotency-and-reliability.md](idempotency-and-reliability.md) — how the pipeline survives failure: delivery guarantees, idempotency, effectively-once
8. [design-decisions.md](design-decisions.md) — *why* every choice was made
9. [glossary.md](glossary.md) — keep open as a lookup throughout (not a linear read)
10. [open-questions.md](open-questions.md) — what's left / next steps

**Optional deep-dive (advanced):**
[multi-broker-setup.md](multi-broker-setup.md) — simulating a 3-broker cluster locally +
how broker scaling actually works in production (controllers vs brokers, reassignment,
Cruise Control / operators / managed Kafka).

The **-explained** docs teach the concepts from zero; the **-reference** docs are the
practical "how to use it + syntax" companions.

## Index

| Doc | What's in it |
|---|---|
| [overview.md](overview.md) | What this project is, the big picture, who it's for |
| [design-decisions.md](design-decisions.md) | Every decision we made + the reasoning + trade-offs + what makes it robust |
| [kafka-explained.md](kafka-explained.md) | Ground-up *concept* intro to Kafka, KafkaTemplate, how it's set up here |
| [kafka-reference.md](kafka-reference.md) | Kafka *reference*: capabilities, CLI/config syntax, delivery semantics, how our code connects |
| [consumer-explained.md](consumer-explained.md) | Ground-up *concept* intro to the Python consumer, consumer groups, offsets, OpenSearch |
| [opensearch-reference.md](opensearch-reference.md) | OpenSearch *reference*: data model, Query DSL syntax, aggregations, kNN, how our code connects |
| [scaling-and-backpressure.md](scaling-and-backpressure.md) | Runtime behavior under load: backpressure mechanism, consumer rebalancing, adding partitions/brokers + the gotchas |
| [idempotency-and-reliability.md](idempotency-and-reliability.md) | Failure handling: delivery guarantees, where duplicates come from, Kafka vs. our idempotency, effectively-once, how it enables scaling |
| [multi-broker-setup.md](multi-broker-setup.md) | *Advanced:* simulating a 3-broker cluster locally + how production scales brokers (controllers vs brokers, reassignment, tooling) |
| [glossary.md](glossary.md) | Quick-reference vocabulary |
| [open-questions.md](open-questions.md) | Things we haven't done yet / decisions still open / next steps |

## The one-paragraph summary

A real-time, distributed log-processing pipeline that runs entirely on a laptop.
A **Java Spring Boot** app simulates a flood of application logs and pushes them
into **Apache Kafka**. A **Python** worker pulls them off Kafka, batches them, and
indexes them into **OpenSearch** for full-text + structured search. The goal is to
demonstrate distributed-systems skills (decoupling, fault tolerance, backpressure,
horizontal scaling) — not the AI itself. Vector embeddings are an *optional* phase-2
enrichment, deliberately not the backbone, because logs are better served by
full-text + filtered search.

```
[Java Producer] --> [ Apache Kafka ] --> [ Python Worker ] --> [ OpenSearch ]
 simulated logs     buffer / shock        batch + index         search engine
 (Spring Boot)      absorber              (consumer group)      AND storage
                          |
                          +--> [ logs-dlq ]  (dead-letter queue for bad messages)
```
