# LogStream Pipeline — Documentation & Learning Log

This folder captures everything we've figured out while designing this project:
what we're building, why we made each decision, what makes those decisions more
robust, and ground-up explanations of the moving parts.

It doubles as **interview prep** — every design choice here has a "why" you can
defend out loud.

## Index

| Doc | What's in it |
|---|---|
| [overview.md](overview.md) | What this project is, the big picture, who it's for |
| [design-decisions.md](design-decisions.md) | Every decision we made + the reasoning + trade-offs + what makes it robust |
| [kafka-explained.md](kafka-explained.md) | Ground-up explanation of Kafka, KafkaTemplate, how it's set up here |
| [consumer-explained.md](consumer-explained.md) | Ground-up explanation of the Python consumer, consumer groups, offsets, OpenSearch |
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
