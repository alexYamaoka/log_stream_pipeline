"""
Kafka -> OpenSearch worker.

Pulls log events from the "raw-logs" topic, batches them, and bulk-indexes them into
OpenSearch for full-text + structured search. Bad messages are routed to a dead-letter
topic instead of crashing the worker.

Distributed-systems features on display:
  * Consumer group       -> run multiple copies, Kafka load-balances partitions
  * Manual offset commit -> only commit AFTER a batch is safely indexed
  * Dead-letter queue    -> corrupt messages go to "logs-dlq", never lost
  * Backpressure         -> if OpenSearch is slow, we simply consume slower
"""

import json
import os
import signal
import sys

from kafka import KafkaConsumer, KafkaProducer
from opensearchpy import OpenSearch, helpers

# --- Config (override via env vars) ------------------------------------------
BOOTSTRAP = os.getenv("BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("TOPIC", "raw-logs")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "logs-dlq")
GROUP_ID = os.getenv("GROUP_ID", "log-indexers")
INDEX = os.getenv("INDEX", "logs")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "200"))
OPENSEARCH_URL = os.getenv("OPENSEARCH_URL", "http://localhost:9200")


# --- OpenSearch index mapping ------------------------------------------------
# Explicit mapping = the "schema". keyword fields are for exact filters/aggregations,
# text fields are analyzed for full-text search.
def index_mapping():
    return {
        "mappings": {
            "properties": {
                "id": {"type": "keyword"},
                "timestamp": {"type": "date", "format": "epoch_millis"},
                "service": {"type": "keyword"},
                "host": {"type": "keyword"},
                "level": {"type": "keyword"},
                "message": {"type": "text"},
            }
        }
    }


def ensure_index(client):
    if not client.indices.exists(INDEX):
        client.indices.create(INDEX, body=index_mapping())
        print(f"created index '{INDEX}'")


def to_action(doc):
    """Turn a parsed log dict into an OpenSearch bulk-index action."""
    return {"_index": INDEX, "_id": doc["id"], "_source": doc}


def main():
    client = OpenSearch(OPENSEARCH_URL)
    ensure_index(client)

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id=GROUP_ID,
        enable_auto_commit=False,          # we commit manually, after indexing
        auto_offset_reset="earliest",
        value_deserializer=lambda b: b.decode("utf-8"),
        max_poll_records=BATCH_SIZE,
    )
    dlq = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: v.encode("utf-8"),
    )

    running = {"on": True}
    signal.signal(signal.SIGINT, lambda *_: running.update(on=False))
    print(f"worker up. group='{GROUP_ID}' topic='{TOPIC}' -> index '{INDEX}'. Ctrl-C to stop.")

    total = 0
    while running["on"]:
        # poll() returns {partition: [records]}; timeout lets us check the stop flag.
        batches = consumer.poll(timeout_ms=1000)
        actions = []
        for records in batches.values():
            for record in records:
                try:
                    actions.append(to_action(json.loads(record.value)))
                except Exception as e:
                    # Corrupt/unparseable message -> dead-letter queue, keep going.
                    dlq.send(DLQ_TOPIC, f'{{"error":"{e}","raw":{json.dumps(record.value)}}}')

        if actions:
            helpers.bulk(client, actions)        # one bulk request for the whole batch
            consumer.commit()                    # only NOW mark these as processed
            total += len(actions)
            print(f"  indexed {len(actions)} (total {total})")

    consumer.close()
    dlq.flush()
    print(f"stopped. {total} logs indexed total.")


if __name__ == "__main__":
    sys.exit(main())
