# Kafka — Reference & How-To

A practical reference: what Kafka is, what you can do with it, the command/config
**syntax**, and exactly how our code connects to and uses it. For the gentle
conceptual intro, read [kafka-explained.md](kafka-explained.md) first.

---

## 1. What Kafka is (one paragraph)

Apache Kafka is a **distributed, append-only commit log** that you use as a message
broker. Producers append messages to the end of a **topic**; consumers read forward
through it at their own pace. Messages are persisted to disk and retained for a
configurable time (default 7 days) — *not* deleted on read. That retention + the
append-only design is what gives Kafka its replay, fault-tolerance, and multi-consumer
properties.

## 2. Core concepts (the data model)

```
Topic "raw-logs"
├── Partition 0:  [msg0][msg1][msg2][msg3] ...   ← each msg has an offset (0,1,2,3)
├── Partition 1:  [msg0][msg1][msg2] ...
└── Partition 2:  [msg0][msg1] ...
```

- **Topic** — a named stream of messages (`raw-logs`, `logs-dlq`).
- **Partition** — an ordered, independent sub-log of a topic. The **unit of parallelism
  and ordering**: order is guaranteed *within* a partition, not across partitions.
- **Offset** — a message's position in its partition. Consumers track "how far I've read"
  as a committed offset.
- **Message** — a record with an optional **key**, a **value** (the payload), headers,
  and a timestamp.
- **Producer** — appends messages. Chooses the partition (by key hash, or round-robin).
- **Consumer** — reads messages. Tracks offsets.
- **Consumer group** — a set of consumers sharing a `group.id`; Kafka assigns each
  partition to exactly one member and rebalances on change.
- **Broker** — a Kafka server. A **cluster** is several brokers; partitions are spread
  (and replicated) across them. We run one broker.

### How the key chooses a partition
```
partition = hash(key) % number_of_partitions
```
Same key → same partition → guaranteed order for that key. In our code the key is the
log's `id`, so each log's events stay ordered relative to themselves.

## 2b. Clusters, replication, leaders & followers (redundancy)

A **cluster** is several brokers (machines). Two different things spread across them:
- **Partitions** spread the *data* → **scaling** (more brokers/partitions = more
  throughput & storage).
- **Replicas** duplicate each partition onto *other* brokers → **redundancy / HA**.

**Replication factor (RF)** = how many copies of each partition exist, on different
brokers. RF=3 → 3 copies. For each partition:
- one replica is the **leader** — handles ALL reads & writes;
- the others are **followers** — passively copy the leader to stay current;
- followers that are caught up form the **in-sync replicas (ISR)**.

**Failover:** if a leader's broker dies, Kafka auto-promotes an in-sync follower to
leader → no downtime, no loss. (This is exactly what `acks=all` leans on — it waits for
the ISR to also have the message.)

**Key insight — scaling and backup are the SAME machines.** Every broker is
simultaneously the *leader* for some partitions (doing work) and a *follower* for others
(holding backups). No broker is "just a backup." Example — 3 brokers, 3 partitions, RF=2:
```
            P0          P1          P2
Broker 1:   LEADER      follower    —
Broker 2:   —           LEADER      follower
Broker 3:   follower    —           LEADER
```
Each broker leads one partition and backs up another. Lose Broker 1 → P0's follower on
Broker 3 is promoted to leader; P0 stays available.

**Two independent dials:**

| You want… | Turn up… |
|---|---|
| Scale (throughput, storage) | brokers + partitions |
| Safety (survive a failure) | replication factor |

**Our setup:** 1 broker, RF=1 → no followers, no redundancy, no scaling. A single-node
*simulation* — same code/concepts as production, none of the HA benefits. (Also why
`acks=1` and `acks=all` behave identically here — there are no replicas to wait for.)

## 3. What you can do with Kafka (capabilities)

- **Decouple** producers from consumers (different speeds, languages, lifecycles).
- **Buffer / absorb spikes** — the backlog sits on disk safely.
- **Replay** — reset a consumer's offset to re-read old messages (reprocessing, backfills).
- **Fan-out** — many independent consumer groups each get the full stream.
- **Scale horizontally** — add partitions + consumers to parallelize.
- **Retain & compact** — time-based retention, or log compaction (keep latest per key).
- **Stream processing** — Kafka Streams / ksqlDB for transformations (not used here).

## 4. Operating Kafka — CLI syntax

Kafka ships shell tools inside the container at `/opt/kafka/bin/`. Run them via
`docker exec`. (Our broker listens on `localhost:9092`.)

```bash
# List all topics
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list

# Describe a topic (partitions, replicas, leaders)
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic raw-logs

# Create a topic with 3 partitions (to enable parallel consumers)
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --topic raw-logs --partitions 3

# Peek at messages on a topic (great for debugging)
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic raw-logs --from-beginning --max-messages 5

# Inspect a consumer group — shows CURRENT-OFFSET, LOG-END-OFFSET, and LAG
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group log-indexers
```

> **LAG** (from the last command) is the single most useful Kafka health metric:
> `LAG = log-end-offset − current-offset` = how many messages the consumer still
> hasn't processed. Lag climbing = consumer falling behind (backpressure in action).

## 5. The config syntax we use

### Producer side — `producer-java/src/main/resources/application.yml`
```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092     # broker address(es)
    producer:
      key-serializer:   ...StringSerializer   # how to turn the key into bytes
      value-serializer: ...StringSerializer   # how to turn the value into bytes
      acks: "1"               # durability: 0=fire&forget, 1=leader ack, all=full replication
      batch-size: 65536       # bytes to accumulate per partition before sending
      properties:
        linger.ms: 20         # wait up to 20ms to fill a batch (throughput vs latency)
        compression.type: snappy   # compress each batch (snappy|gzip|lz4|zstd|none)
```
Key producer knobs and what they trade:
- **`acks`** — higher = safer but slower. `1` is a good default.
- **`linger.ms` + `batch.size`** — bigger batches = more throughput, slightly more latency.
- **`compression.type`** — saves network/disk; consumer must have the matching codec
  (this is why we added `python-snappy` on the Python side).

### Consumer side — `consumer-python/consumer.py`
```python
KafkaConsumer(
    "raw-logs",
    bootstrap_servers="localhost:9092",
    group_id="log-indexers",          # consumer-group membership
    enable_auto_commit=False,         # we commit offsets manually
    auto_offset_reset="earliest",     # where to start with no committed offset
    value_deserializer=lambda b: b.decode("utf-8"),  # bytes -> str
    max_poll_records=200,             # max messages returned per poll()
)
```

## 5b. Acknowledgments: producer `acks` vs consumer commits

Kafka has **two completely different "acknowledgment" mechanisms**, and they're easy to
confuse. One is on the **push** side, one on the **pull** side.

### Producer `acks` (PUSH side) — "did the broker safely store my write?"
When the producer sends a record, `acks` controls **how many broker replicas must confirm
the write** before `send()` is treated as successful. It's about **write durability**.

| `acks` | Producer waits for | Durability | Speed |
|---|---|---|---|
| `0` | nothing (fire-and-forget) | can silently lose data | fastest |
| `1` | the partition **leader** only | tiny loss window if leader dies right after acking | balanced ← **our setting** |
| `all` (`-1`) | leader **+ all in-sync replicas** | no loss while ≥1 replica survives | slowest |

**The flow:**
```
producer ──send──► partition leader appends to its log
                 └─(acks=all only)─► waits for follower replicas to copy it
        ◄──ack─── leader confirms
producer's send() future completes ✓   (no ack before timeout → producer retries)
```

**How it's handled in code:** it's pure **configuration** — `acks: "1"` in
`application.yml`. The Kafka client library performs the wait/retry protocol internally;
**you write no acknowledgment code.** `kafkaTemplate.send()` returns a future that
completes when the ack arrives.

> ⚠️ In our **single-broker** dev setup there are no replicas, so `1` and `all` behave
> identically here. The difference only matters in a multi-broker cluster.
> Also note: `enable.idempotence=true` requires `acks=all` — see
> [idempotency-and-reliability.md](idempotency-and-reliability.md) §5.

### Consumer commit (PULL side) — "how far have I processed?"
The consumer's "acknowledgment" is a **separate** thing: **committing offsets**. It is
*not* the `acks` config. After processing messages, the consumer tells Kafka "I'm done up
to offset N" so it won't re-deliver them. We do this manually
(`enable_auto_commit=False` + `consumer.commit()` after indexing). See §7.

### Side-by-side
| | Producer `acks` | Consumer commit |
|---|---|---|
| Which side | **Push** (producing into Kafka) | **Pull** (consuming from Kafka) |
| Answers | "Did the broker store my write durably?" | "How far have I processed?" |
| Mechanism | `acks` config value | offset commit |
| In our code | `application.yml` → `acks: "1"` (declarative config) | `consumer.commit()` (explicit call) |
| Protects | Write durability (no loss on the way in) | Progress tracking → at-least-once |

So: **`acks` is for pushing, not pulling.** The pull-side "acknowledge I processed this"
is the offset commit, which is the only acknowledgment logic we actually *write*.

## 6. How our code connects to & uses Kafka

### Producing (Java)
Spring Boot reads the YAML and auto-builds a `KafkaTemplate`. We call:
```java
kafkaTemplate.send(TOPIC, key, value);   // returns a CompletableFuture
// e.g. kafkaTemplate.send("raw-logs", "abc-123", "{...json...}")
kafkaTemplate.flush();                    // force-send any buffered batch now
```
`send()` is asynchronous (buffered, batched). `flush()` blocks until everything
buffered is actually sent — we call it before exiting in spike mode.

### Consuming (Python)
```python
batches = consumer.poll(timeout_ms=1000)   # dict: {TopicPartition: [records]}
for records in batches.values():
    for record in records:
        record.value      # the message payload (already decoded to str)
        record.key        # the message key
        record.offset     # this message's offset
        record.partition  # which partition it came from
...
consumer.commit()         # commit the latest read offsets for this group
```

### Dead-letter producing (Python)
We also create a `KafkaProducer` to push bad messages to `logs-dlq`:
```python
dlq = KafkaProducer(bootstrap_servers=BOOTSTRAP,
                    value_serializer=lambda v: v.encode("utf-8"))
dlq.send("logs-dlq", '{"error": "...", "raw": "..."}')
dlq.flush()
```

## 7. Delivery semantics (important interview topic)

| Semantic | How you get it | Our pipeline |
|---|---|---|
| **At-most-once** | Commit offset *before* processing | ✗ (would risk loss) |
| **At-least-once** | Commit offset *after* processing | ✓ (we do this) |
| **Exactly-once** | Transactions + idempotent producer | not needed (we use idempotent writes instead) |

We choose **at-least-once** (commit after `helpers.bulk` succeeds) and make duplicates
harmless by using the log `id` as the OpenSearch `_id` (idempotent writes). See
[design-decisions.md](design-decisions.md) #12 and #14.

## 8. Gotchas we hit
- **Compression codec mismatch** — the producer used `snappy`; the Python consumer
  needs a snappy library (`python-snappy`) to *decompress*. Codecs must exist on both
  sides. (Alternatives needing no native lib: `gzip` on the Python side is stdlib.)
- **Single partition** — with 1 partition, only one consumer in a group does work.
  Create the topic with more partitions to demonstrate parallelism.
