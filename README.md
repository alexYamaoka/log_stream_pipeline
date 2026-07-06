# LogStream Pipeline

**A real-time, distributed log-processing pipeline you can run on a laptop.**

High-volume application logs are ingested, buffered, processed, and made instantly
searchable — using the same architecture you'd deploy in production, scaled down to a
single machine.

Logs are generated at high speed by a **Java (Spring Boot)** service, buffered through
**Apache Kafka**, then pulled, batched, and indexed by a **Python** worker into
**OpenSearch**, where they become instantly searchable.

The goal isn't AI for its own sake — it's to **demonstrate distributed-systems
engineering**: decoupling, fault tolerance, backpressure, and horizontal scaling, all
runnable locally with zero cloud cost.

---

## Architecture

```
[Java Producer] ──► [ Apache Kafka ] ──► [ Python Worker ] ──► [ OpenSearch ]
 simulated logs      buffer / shock        batch + index         search engine
 (Spring Boot)       absorber              (consumer group)      AND storage
                          │
                          └──► [ logs-dlq ]   dead-letter queue (bad messages)
```

| Stage | Tech | Role |
|-------|------|------|
| **Ingestor** | Java 21 · Spring Boot · spring-kafka | Generate realistic logs and stream them into Kafka |
| **Broker** | Apache Kafka (KRaft) | Buffer messages on disk so nothing drops under load |
| **Processor** | Python · kafka-python · opensearch-py | Pull, batch, and index logs (optionally embed them) |
| **Storage** | OpenSearch (+ Dashboards) | Full-text + structured search; also the datastore |

## Why it's built this way

Every design choice has a deliberate reason — these are the highlights:

- **Kafka in the middle** decouples ingestion from processing, so either side can crash,
  slow down, or scale independently without affecting the other.
- **OpenSearch, not a vector database.** Logs are structured and keyword-heavy, so
  full-text + filtered search is the right backbone. Vector embeddings are an *optional*
  phase-2 enrichment, not the foundation.
- **At-least-once delivery.** The worker commits its Kafka offset *only after* a batch is
  safely indexed — combined with idempotent writes, the pipeline never loses a message.
- **Dead-letter queue.** Unprocessable messages are quarantined instead of crashing the
  pipeline.
- **Horizontal scaling for free.** Run multiple workers in one Kafka consumer group and
  Kafka auto-balances the load.

The full reasoning lives in **[`docs/`](docs/)**, organized into three files:
- **[docs/PROJECT.md](docs/PROJECT.md)** — overview, every design decision + trade-offs, next steps
- **[docs/DISTRIBUTED-SYSTEMS.md](docs/DISTRIBUTED-SYSTEMS.md)** — how Kafka, OpenSearch, reliability & scaling work (from zero) + glossary
- **[docs/SYSTEM-DESIGN-NOTES.md](docs/SYSTEM-DESIGN-NOTES.md)** — capacity estimation + scaling playbook (interview prep)

## Distributed-systems concepts demonstrated

| Concept | Where it lives |
|---|---|
| Decoupling / async buffering | Kafka sits between producer and worker |
| Fault tolerance | Kill the worker mid-stream; Kafka holds the data; restart resumes with no loss |
| Backpressure | Manual offset commit — only commit after a batch is indexed |
| Horizontal scaling | Multiple `consumer.py` in one `GROUP_ID` → Kafka balances partitions |
| Dead-letter queue | Corrupt messages routed to `logs-dlq`, never dropped |

---

## Quickstart

### Prerequisites
- Docker Desktop
- **JDK 21 recommended** (Spring Boot 3.3 supports JDK 17–22). In IntelliJ:
  *File → Project Structure → SDKs → +* to download one — no manual install needed.
- IntelliJ IDEA (bundles Maven, so no separate `mvn` install required).
  For the terminal path instead, install Maven: `brew install maven`.
- Python 3.10+
- *(Phase 2 only)* [Ollama](https://ollama.com) with `ollama pull all-minilm`

### Setup (one-time)
Do this once after cloning. It pulls the Docker images and creates the Python
virtual environment for the worker.

```bash
# from the repo root:

# 1. Pre-pull the infrastructure images (Kafka, OpenSearch, Dashboards)
docker compose pull

# 2. Create the Python virtual environment and install the worker's dependencies
cd consumer-python
python3 -m venv .venv            # create an isolated environment in .venv/
source .venv/bin/activate        # activate it (prompt shows "(.venv)")
pip install --upgrade pip
pip install -r requirements.txt  # installs kafka-python-ng, opensearch-py, requests
cd ..
```
> The JDK is configured inside IntelliJ (JDK 21) — see Prerequisites above; nothing to
> install on the command line for the Java side.

### Running it
Each run uses three terminals. Start them in this order.

**Terminal 1 — infrastructure:**
```bash
docker compose up -d
curl http://localhost:9200        # sanity check: OpenSearch should respond with JSON
```
- OpenSearch API: http://localhost:9200 · Dashboards UI: http://localhost:5601 · Kafka: localhost:9092

**Terminal 2 — Python worker** (re-activate the venv in every new terminal):
```bash
cd consumer-python
source .venv/bin/activate         # ← required each new terminal session
python consumer.py
```
It will print `worker up ...` and wait for messages.

**Terminal 3 — Java producer (Spring Boot):**
- **In IntelliJ:** open `producer-java/pom.xml` as a project, then run
  `ProducerApplication` (green ▶ next to `main`). Set the workload in the Run config's
  *Program arguments*, e.g. `--app.mode=spike --app.count=10000`.
- **From the terminal** (requires `brew install maven`):
  ```bash
  cd producer-java

  # Steady stream — one log every 500ms (default):
  mvn spring-boot:run

  # OR the stress test — blast 10,000 logs as fast as possible:
  mvn spring-boot:run -Dspring-boot.run.arguments="--app.mode=spike --app.count=10000"
  ```

Once logs are flowing, Terminal 2 prints `indexed N (total N)` lines.

### Search your logs
```bash
# Count everything indexed
curl "http://localhost:9200/logs/_count"

# All ERROR logs from payment-api (structured filter)
curl -s "http://localhost:9200/logs/_search" -H 'Content-Type: application/json' -d '{
  "query": {"bool": {"filter": [
    {"term": {"level": "ERROR"}},
    {"term": {"service": "payment-api"}}
  ]}}
}'

# Full-text search for "timeout" in the message
curl -s "http://localhost:9200/logs/_search" -H 'Content-Type: application/json' -d '{
  "query": {"match": {"message": "timeout"}}
}'
```
Or explore visually in Dashboards (http://localhost:5601 → Discover).

## Project structure
```
.
├── docker-compose.yml      # Kafka + OpenSearch + Dashboards
├── producer-java/          # Spring Boot Kafka producer (the ingestor)
├── consumer-python/        # Python Kafka→OpenSearch worker (the processor)
└── docs/                   # PROJECT · DISTRIBUTED-SYSTEMS · SYSTEM-DESIGN-NOTES
```

## Phase 2 (optional): semantic search
```bash
ollama pull all-minilm
USE_EMBEDDINGS=true python consumer.py
```
Attaches a 384-dim vector to each log, enabling kNN "find logs similar to this error"
queries alongside the full-text backbone (**hybrid search**).

## Shut down
```bash
docker compose down        # keep data
docker compose down -v     # also wipe Kafka + OpenSearch volumes
```

---

## Status

Early scaffolding — the architecture and code are in place but **not yet verified
end-to-end**. See the "Open Questions & Next Steps" section of
[`docs/PROJECT.md`](docs/PROJECT.md) for the current checklist and next steps. This is a learning + portfolio project; it *simulates* a
distributed cluster on a single machine (one broker, one node, one partition) using the
same code and architecture you'd deploy at scale.
