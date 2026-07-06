# LogStream Pipeline

A local, distributed log pipeline built to learn Kafka and message-queue-based system
design.

A Java (Spring Boot) service generates log events and publishes them to Kafka. A Python
worker consumes them, batches them, and indexes them into OpenSearch for search.
Everything runs locally in Docker.

This is a learning project. The point is the architecture and the distributed-systems
concepts it demonstrates — the data is simulated.

## Architecture

```
[Java producer] --> [ Kafka ] --> [ Python worker ] --> [ OpenSearch ]
 generates logs     message queue   consume + index      search + storage
                        |
                        +--> [ logs-dlq ]  dead-letter queue for bad messages
```

| Component | Technology | Responsibility |
|-----------|------------|----------------|
| Producer | Java 21, Spring Boot, spring-kafka | Generate log events and publish to the `raw-logs` topic |
| Broker | Apache Kafka (KRaft) | Buffer messages on disk between producer and consumer |
| Consumer | Python, kafka-python-ng, opensearch-py | Consume, batch, and bulk-index into OpenSearch |
| Storage | OpenSearch (+ Dashboards) | Full-text and structured search; also the datastore |

## Design notes

- **Kafka between producer and consumer** decouples them. Either side can stop, slow
  down, or scale independently; Kafka holds messages on disk in between.
- **OpenSearch as the store.** Logs are structured and keyword-heavy, so full-text +
  filtered search fits better than a plain key-value store. OpenSearch is both the search
  engine and the datastore (no separate database).
- **Manual offset commit, after indexing.** The consumer commits its Kafka offset only
  after a batch is safely indexed. On a crash it re-reads from the last commit, so no
  message is lost (at-least-once delivery). Re-indexing is safe because each log's `id` is
  the OpenSearch document id, so a repeat overwrites rather than duplicates.
- **Dead-letter queue.** A message that fails to parse is routed to `logs-dlq` instead of
  crashing the worker.
- **Consumer group.** Running multiple copies of the worker in the same group makes Kafka
  split the partitions across them (parallel processing).

## Concepts demonstrated

| Concept | Where |
|---------|-------|
| Decoupling / buffering | Kafka sits between producer and consumer |
| Fault tolerance | Stop the worker mid-run; Kafka keeps the data; restart resumes |
| Backpressure | Commit only after indexing; a slow sink slows the consumer, not the producer |
| Horizontal scaling | Multiple workers in one `GROUP_ID`; Kafka balances partitions |
| Dead-letter queue | Bad messages routed to `logs-dlq` |

## Repository layout

```
.
├── docker-compose.yml   # Kafka + OpenSearch + Dashboards
├── producer-java/       # Spring Boot Kafka producer
├── consumer-python/     # Python Kafka -> OpenSearch worker
└── docs/                # detailed notes (see Documentation below)
```

## Running it

### Prerequisites
- Docker Desktop
- JDK 21 (Spring Boot 3.3 supports JDK 17–22). In IntelliJ you can download it via
  File → Project Structure → SDKs.
- IntelliJ IDEA (bundles Maven), or install Maven separately for the terminal path.
- Python 3.10+

### Setup (once)
```bash
# pull infrastructure images
docker compose pull

# create the Python virtual environment for the worker
cd consumer-python
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
cd ..
```

### Run (three terminals)

1. Infrastructure:
   ```bash
   docker compose up -d
   curl http://localhost:9200      # OpenSearch should respond with JSON
   ```
   OpenSearch: http://localhost:9200 · Dashboards: http://localhost:5601 · Kafka: localhost:9092

2. Python worker (re-activate the venv in each new terminal):
   ```bash
   cd consumer-python
   source .venv/bin/activate
   python consumer.py
   ```

3. Java producer:
   - IntelliJ: open `producer-java/pom.xml`, run `ProducerApplication`. Set the workload in
     the run config's program arguments, e.g. `--app.mode=spike --app.count=10000`.
   - Terminal (needs Maven):
     ```bash
     cd producer-java
     mvn spring-boot:run                                                    # steady stream
     mvn spring-boot:run -Dspring-boot.run.arguments="--app.mode=spike --app.count=10000"  # burst
     ```

### Search the results
```bash
# count indexed logs
curl "http://localhost:9200/logs/_count"

# ERROR logs from payment-api (structured filter)
curl -s "http://localhost:9200/logs/_search" -H 'Content-Type: application/json' -d '{
  "query": {"bool": {"filter": [
    {"term": {"level": "ERROR"}},
    {"term": {"service": "payment-api"}}
  ]}}
}'

# full-text search on the message
curl -s "http://localhost:9200/logs/_search" -H 'Content-Type: application/json' -d '{
  "query": {"match": {"message": "timeout"}}
}'
```
Or browse in Dashboards (http://localhost:5601 → Discover; create a data view on the
`logs` index with `timestamp` as the time field).

### Shut down
```bash
docker compose down        # keep data
docker compose down -v     # also wipe the Kafka + OpenSearch volumes
```

## Status

Scaffolding — the code and architecture are in place but not yet fully verified
end-to-end. This runs one of everything (one broker, one OpenSearch node, one partition),
so it simulates a distributed system on a single machine rather than being one. The code
and architecture are the same as you'd run at scale.

## Documentation

Detailed notes are in `docs/`, in three files:

- **[docs/PROJECT.md](docs/PROJECT.md)** — overview, design decisions and trade-offs, open questions
- **[docs/DISTRIBUTED-SYSTEMS.md](docs/DISTRIBUTED-SYSTEMS.md)** — how Kafka and OpenSearch work, reliability, scaling (from first principles), plus a glossary
- **[docs/SYSTEM-DESIGN-NOTES.md](docs/SYSTEM-DESIGN-NOTES.md)** — capacity estimation and scaling notes for system-design study
