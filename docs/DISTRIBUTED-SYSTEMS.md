# Distributed Systems — How the Pipeline & Its Tech Work

Consolidated deep-dive learnings from building this project: Kafka, OpenSearch,
reliability, and scaling — from first principles, tied to the actual code.

**Companion docs:** `PROJECT.md` (the project) · `SYSTEM-DESIGN-NOTES.md` (interview prep).

## Contents
1. **Kafka, Explained From Zero** — conveyor-belt model, KafkaTemplate, our setup
2. **Kafka — Reference & How-To** — data model, CLI/config syntax, acks, replication, delivery semantics
3. **The Python Consumer & OpenSearch, Explained From Zero** — consumer groups, offsets, DLQ
4. **OpenSearch — Reference & How-To** — data model, Query DSL, aggregations, kNN
5. **Idempotency & Reliability** — delivery guarantees, effectively-once
6. **Scaling & Backpressure** — backpressure, rebalancing, safe restarts
7. **Multi-Broker Kafka** — local 3-broker sim + production scaling
8. **Glossary**

---

# Kafka, Explained From Zero

No assumed knowledge. Builds up from "what is Kafka" to "what is KafkaTemplate and
how is it wired into our Spring Boot app."

## 1. What is Kafka, really?

Imagine a **conveyor belt** in a factory. One worker puts boxes on; other workers
down the line take boxes off and process them. The two don't move at the same speed —
if the taker is slow, boxes pile up on the belt and wait. Nobody stops. Nothing
falls on the floor.

**Kafka is that conveyor belt for data.** One program writes messages onto it; other
programs read messages off it; Kafka stores them safely in between (on disk, so a
crash doesn't lose them).

The technical term is a **message broker** / **event streaming platform**. The point:
let the *sender* and *receiver* run independently, at different speeds, without
talking to each other directly.

In our project:
- **Sender** = Java Spring Boot app (the **producer**)
- **Conveyor belt** = Kafka
- **Receiver** = Python app (the **consumer**)

## 2. The vocabulary (6 words)

| Term | Plain meaning | In our project |
|---|---|---|
| **Broker** | The Kafka server — the machine running the belt | The `kafka` Docker container |
| **Topic** | A named belt; messages go to a topic | `raw-logs` (and `logs-dlq`) |
| **Producer** | A program that *writes* to a topic | The Java app |
| **Consumer** | A program that *reads* from a topic | The Python app |
| **Partition** | A topic split into lanes so work can be shared | We have 1 lane |
| **Offset** | A message's position number in the lane (#0, #1, #2…) | How a consumer bookmarks "read up to here" |

**The magic property:** when a consumer reads a message, Kafka **doesn't delete it.**
The message stays on the belt. The consumer keeps a bookmark (the **offset**) saying
"I've processed up to #4,071." If it crashes and restarts, it reads the bookmark and
continues from #4,072 — no loss, no reprocessing. That's the famous fault tolerance.

## 3. How Kafka is set up here (Docker)

From `docker-compose.yml`, the `kafka:` block. Key lines translated:

```yaml
image: apache/kafka:3.8.0      # official Kafka
ports:
  - "9092:9092"                # the door programs knock on to send/receive
```
`9092` is the address apps connect to. When Java says "connect to `localhost:9092`,"
this is what it reaches.

```yaml
KAFKA_PROCESS_ROLES: broker,controller
KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
```
History: Kafka *used* to need a separate program, **ZooKeeper**, for cluster
bookkeeping. Modern Kafka replaced it with a built-in mechanism called **KRaft**.
These lines say "this one container is *both* the broker (port 9092) **and** its own
controller/bookkeeper (port 9093)." In production you'd run several brokers on
several machines; here one container does it all — which is why it can feel
"distributed" on one laptop (same software, single node).

```yaml
KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
```
Why we never manually created `raw-logs`: the first time Java sends to it, Kafka
creates the topic on the spot.

```yaml
volumes:
  - kafka-data:/var/lib/kafka/data
```
Where messages physically live on disk. Survives container restarts (unless you
`docker compose down -v`).

**Summary:** `docker compose up` starts one Kafka broker on port 9092 that
auto-creates topics and persists messages to disk.

## 4. How the Java app connects (config)

From `producer-java/src/main/resources/application.yml`:

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: ...StringSerializer
      value-serializer: ...StringSerializer
```

- **`bootstrap-servers: localhost:9092`** — "the belt is at this address." Matches the
  Docker port.
- **Serializers** — Kafka only moves raw **bytes**; it doesn't understand Java objects
  or even text. A *serializer* translates your data into bytes for the trip. We send
  plain strings (JSON logs), so both key and value use `StringSerializer`. The Python
  consumer does the reverse (*deserializes* bytes back to a string).
- Tuning lines (`acks`, `batch-size`, `linger.ms`, `compression.type`) are performance
  knobs: "wait up to 20ms to bundle messages, compress the bundle, require only the
  main broker to confirm." Ignore them while learning.

## 5. What is KafkaTemplate?

Kafka ships a low-level Java tool called `KafkaProducer`. Using it raw means *you*
construct it with a big properties map, manage its lifecycle, close it, handle
threading — lots of boilerplate.

**`KafkaTemplate` is Spring's friendly wrapper around that raw producer.**

> Raw `KafkaProducer` = a manual camera with every dial exposed.
> `KafkaTemplate` = the same camera in point-and-shoot mode. One button: `.send()`.

The surprising part for beginners: **you never create the `KafkaTemplate` yourself.**
Because `spring-kafka` is on the classpath and there's a `spring.kafka` section in
`application.yml`, Spring Boot reads that config at startup, builds a fully-configured
`KafkaTemplate`, and **hands it to you**. That handing-over is **dependency injection**.
See the constructor in `LogProducerRunner.java`:

```java
public LogProducerRunner(KafkaTemplate<String, String> kafkaTemplate, ...) {
    this.kafkaTemplate = kafkaTemplate;   // Spring injects the ready-made one
}
```

You just *ask* for it; Spring supplies it. `<String, String>` = "sends String keys and
String values," matching the serializers.

Sending is one line:
```java
kafkaTemplate.send("raw-logs", key, json);
//                  ^topic      ^partition routing key   ^the message
```
Under the hood: your string → serializer → bytes → shipped to `localhost:9092` →
Kafka appends it to `raw-logs` at the next offset.

## 6. The full picture

```
 JAVA APP                       KAFKA BROKER (Docker)         PYTHON APP
 ────────                       ─────────────────────         ──────────
 kafkaTemplate.send(                                          consumer.poll()
   "raw-logs", key, json) ─bytes─► topic: raw-logs            reads next msgs,
                                     [#0][#1][#2]... ─bytes─►  processes them,
                                     (stored on disk)          saves its offset
```

Producer and consumer **never talk to each other** — they only know the belt. That
separation is the whole reason this is a "distributed system": either side can crash,
slow down, or scale to many copies, and the other neither knows nor cares, because
Kafka holds the data safely in the middle.

See consumer-explained.md for the read side.


---

# Kafka — Reference & How-To

A practical reference: what Kafka is, what you can do with it, the command/config
**syntax**, and exactly how our code connects to and uses it. For the gentle
conceptual intro, read kafka-explained.md first.

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
> idempotency-and-reliability.md §5.

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
design-decisions.md #12 and #14.

## 8. Gotchas we hit
- **Compression codec mismatch** — the producer used `snappy`; the Python consumer
  needs a snappy library (`python-snappy`) to *decompress*. Codecs must exist on both
  sides. (Alternatives needing no native lib: `gzip` on the Python side is stdlib.)
- **Single partition** — with 1 partition, only one consumer in a group does work.
  Create the topic with more partitions to demonstrate parallelism.


---

# The Python Consumer & OpenSearch, Explained From Zero

The read side of the pipeline. Covers consumer groups, offsets/commits, batching,
the dead-letter queue, and how logs land in OpenSearch. Pairs with
kafka-explained.md.

## 1. What is a consumer's job?

Back to the conveyor belt. The producer (Java) puts boxes on. The **consumer** is the
worker at the other end taking boxes off and doing something with each — here,
**parse the log, then save it into OpenSearch so it's searchable.**

The consumer's whole life is a loop:
> *ask Kafka for new messages → process them → tell Kafka "done up to here" → repeat.*

Everything in `consumer.py` hangs off that loop.

## 2. Creating the consumer

```python
consumer = KafkaConsumer(
    TOPIC,                              # "raw-logs" — which belt
    bootstrap_servers=BOOTSTRAP,        # localhost:9092 — same broker as Java
    group_id=GROUP_ID,                  # "log-indexers" — the team name (Part 3)
    enable_auto_commit=False,           # WE control the bookmark (Part 5)
    auto_offset_reset="earliest",       # no bookmark yet? start at msg #0
    value_deserializer=lambda b: b.decode("utf-8"),  # bytes -> string
    max_poll_records=BATCH_SIZE,        # give me up to 200 at a time (Part 6)
)
```

- **`value_deserializer`** — Kafka moves bytes; Java serialized string→bytes, here we
  deserialize bytes→string with `.decode("utf-8")`. The translator in reverse.
- **`auto_offset_reset="earliest"`** — first connect with no bookmark: start from the
  very beginning so you don't miss logs sent before the consumer started.
- **`max_poll_records`** — "don't dump the whole belt; at most 200 per request." Enables
  batching.

## 3. Consumer groups — the scaling superpower

`group_id="log-indexers"` gives the consumer a **team name**. Any consumer with the
same `group_id` is on the same team. Kafka's rule:

> **Within one group, each message goes to exactly ONE team member.**

Kafka hands out lanes (partitions) to team members:

```
1 worker:                          3 workers (same group_id):
 lanes 0,1,2,3 ──► worker A         lane 0   ──► worker A
                                    lane 1   ──► worker B
                                    lanes 2,3 ─► worker C
```

So "run 5 workers in parallel" is literally just:
```bash
python consumer.py   # terminal 1
python consumer.py   # terminal 2   ← same GROUP_ID; Kafka auto-balances
python consumer.py   # terminal 3
```
**No coordination code.** Kafka splits the lanes, and rebalances if one crashes or a
new one joins.

**Current limitation:** we have **1 partition**, so only one worker gets work at a
time. To see real parallelism, create `raw-logs` with multiple partitions (e.g., 3).
The *mechanism* is exactly as described.

A *different* `group_id` is a separate team that gets its **own full copy** of every
message — that's how you'd add an independent analytics consumer without stealing
messages from the indexer.

## 4. The poll loop

```python
while running["on"]:
    batches = consumer.poll(timeout_ms=1000)   # "any new messages? wait up to 1s"
```

`poll()` asks Kafka for messages. `timeout_ms=1000` = "if nothing's there, wait up to
1 second then return empty," so the loop can re-check the `running` flag — which is
what lets **Ctrl-C** shut it down cleanly (the `signal` handler flips `running`).

`poll()` returns `{ partition → [messages] }`. The nested loops flatten it:

```python
for records in batches.values():
    for record in records:
        try:
            actions.append(to_action(json.loads(record.value)))  # string -> dict
        except Exception as e:
            dlq.send(DLQ_TOPIC, ...)   # bad message -> DLQ (Part 7)
```

## 5. The offset bookmark — and why we commit MANUALLY

Kafka never deletes messages; each consumer keeps a **bookmark (offset)**. The
question is *when* to advance it. That's why we set `enable_auto_commit=False`.

The naive default (auto-commit on a timer) might mark messages #0–#199 "done"
*before* they're saved to OpenSearch. A crash in that window = those logs lost
forever (Kafka thinks they're handled).

Our code commits **only after** the save succeeds:
```python
if actions:
    helpers.bulk(client, actions)   # 1. save the whole batch to OpenSearch FIRST
    consumer.commit()               # 2. ONLY NOW advance the bookmark
```

The order is the entire point:
1. **Save first.**
2. **Then** commit the offset.

If the worker dies between poll and commit, the bookmark didn't move → Kafka
re-delivers those messages on restart. This is **at-least-once delivery**: you may
occasionally process a message twice, but you **never lose one**. (Double-saving is
harmless — see Part 8, idempotency.)

This is also **backpressure**: if OpenSearch is slow, `helpers.bulk()` takes longer,
so the loop naturally pulls from Kafka less often. The backlog piles up safely on
the Kafka belt. The system self-regulates with no special code.

## 6. Why batch instead of one-at-a-time?

We collect up to 200 messages, then send them in **one** `helpers.bulk()` call.

Analogy: mailing 200 letters. You could drive to the post office 200 times, or make
one trip with all 200. Each network round-trip to OpenSearch has fixed overhead, so
bundling is dramatically faster. **Batching is the #1 throughput trick in data
pipelines.** (The Java side batches too.)

## 7. The Dead-Letter Queue (DLQ)

If a message is garbage (malformed JSON), we don't want it to crash the worker:
```python
except Exception as e:
    dlq.send(DLQ_TOPIC, f'{{"error":"{e}","raw":...}}')   # quarantine, keep going
```
The bad message goes to a **separate** topic, `logs-dlq` (a quarantine belt), with
the error. The worker moves on. Later you can inspect/replay the DLQ. Nothing lost,
nothing crashes — standard production hygiene.

## 8. The OpenSearch side (where logs land)

`ensure_index` creates the index (≈ a table) once, with an explicit **mapping**
(≈ the schema):
```python
"service": {"type": "keyword"},   # exact-match: filter/group by service
"level":   {"type": "keyword"},   # exact-match: WHERE level = 'ERROR'
"message": {"type": "text"},      # full-text: search FOR words inside it
```

The `keyword` vs `text` distinction is the core Path-A insight:
- **`keyword`** = one exact value. For **filters/aggregations**: "all logs where
  `service = payment-api` AND `level = ERROR`."
- **`text`** = broken into words and indexed for **search**: "messages containing
  `timeout`."

That combination — exact filters + full-text — is why OpenSearch fits logs better
than a pure vector DB. Embeddings are the optional `USE_EMBEDDINGS` phase-2 layer.

`to_action` packages each log for `helpers.bulk`:
```python
{"_index": "logs", "_id": doc["id"], "_source": doc}
#           index      unique id (= log id)   the data
```
Using the log's own `id` as `_id` is the **idempotency** trick: re-saving the same
log overwrites with identical data, so at-least-once reprocessing creates no
duplicates.

## 9. The full picture (both halves)

```
 JAVA PRODUCER       KAFKA BROKER           PYTHON CONSUMER             OPENSEARCH
 ─────────────       ────────────           ───────────────             ──────────
 generate log ─────► topic: raw-logs ─poll─► json.loads() ─batch─► helpers.bulk() ─► "logs" index
 .send()             [#0][#1][#2]...        (parse)        of 200   (one request)    (searchable)
                         ▲                     │
                         │                     └─bad msg─► topic: logs-dlq (quarantine)
                     offset/bookmark ◄─commit AFTER save succeeds─┘
```

Producer and consumer never touch each other — Kafka is the only thing both know.
The consumer pulls at its own pace, saves safely, and only *then* advances its
bookmark — which makes the whole pipeline lossless even when things crash.


---

# OpenSearch — Reference & How-To

A practical reference: what OpenSearch is, what you can do with it, the query **syntax**
(the Query DSL), and exactly how our code connects to and uses it. The basics of how our
pipeline writes to it are also in consumer-explained.md.

---

## 1. What OpenSearch is (one paragraph)

OpenSearch is a **distributed search engine and document store** built on Apache Lucene
(it's the open-source fork of Elasticsearch). You give it JSON **documents**; it indexes
them so you can run fast **full-text search**, **structured filters**, and **aggregations**
over millions of records. It IS the datastore — there's no separate database. You talk to
it over a **REST/HTTP API** (default port `9200`), sending and receiving JSON.

## 2. Core concepts (the data model)

```
OpenSearch node
└── Index "logs"                    ← like a table
    ├── Mapping                     ← the schema (field names + types)
    └── Documents                   ← JSON records, each with an _id
        ├── { "_id": "abc", "service": "payment-api", "level": "ERROR", ... }
        └── { "_id": "def", ... }
```

- **Index** — a named collection of documents (our `logs` index). Roughly a "table."
- **Document** — one JSON record (one log). Has a unique `_id`.
- **Field** — a key in the document (`service`, `level`, `message`, `timestamp`).
- **Mapping** — the index's schema: each field's **type**, which controls how it's stored
  and queried.
- **Shard** — an index is split into shards (Lucene indexes) for scale; shards can be
  replicated for HA. We run 1 shard, no replicas.
- **Analyzer** — for `text` fields, the process that breaks text into searchable **tokens**
  (lowercasing, splitting on spaces, etc.).

### The field types that matter for logs
| Type | Stored as | Use it for | Example query |
|---|---|---|---|
| `keyword` | one exact, un-analyzed value | filters, aggregations, sorting | `level = "ERROR"` |
| `text` | analyzed into tokens | full-text search | message *contains* "timeout" |
| `date` | timestamp | ranges, time filters, histograms | last 15 min |
| `knn_vector` | float vector | semantic / similarity search | nearest neighbors |

> **`keyword` vs `text` is the single most important distinction.** `keyword` = exact
> match (good for `service`, `level`). `text` = word search (good for `message`). Many
> fields are mapped as *both* in real systems (`message` for search, `message.keyword`
> for aggregation).

## 3. What you can do with OpenSearch (capabilities)

- **Full-text search** — relevance-ranked word search (BM25).
- **Structured filtering** — exact term, ranges, booleans (yes/no, not scored).
- **Aggregations** — group-by / analytics (count errors per service, logs over time).
- **kNN / vector search** — semantic similarity (our optional phase 2).
- **Hybrid search** — combine full-text + vector in one query.
- **Dashboards** — visualize via OpenSearch Dashboards (the UI at :5601).

## 4. Operating OpenSearch — REST syntax (curl)

Everything is HTTP + JSON. The general shape: `METHOD /<index>/<endpoint>`.

```bash
# Cluster health (green/yellow/red)
curl "localhost:9200/_cluster/health?pretty"

# List all indices with doc counts and sizes
curl "localhost:9200/_cat/indices?v"

# See an index's mapping (its schema)
curl "localhost:9200/logs/_mapping?pretty"

# Count documents
curl "localhost:9200/logs/_count"

# Fetch one document by _id
curl "localhost:9200/logs/_doc/<some-id>?pretty"
```

### Creating an index with an explicit mapping
```bash
curl -X PUT "localhost:9200/logs" -H 'Content-Type: application/json' -d '{
  "mappings": {
    "properties": {
      "service":   { "type": "keyword" },
      "level":     { "type": "keyword" },
      "message":   { "type": "text" },
      "timestamp": { "type": "date", "format": "epoch_millis" }
    }
  }
}'
```

### Indexing documents
```bash
# Index/replace a single document with a chosen _id (PUT = idempotent upsert)
curl -X PUT "localhost:9200/logs/_doc/abc-123" -H 'Content-Type: application/json' -d '{
  "service": "payment-api", "level": "ERROR", "message": "Connection timeout"
}'

# Bulk: many actions in one request (newline-delimited JSON — the fast way)
curl -X POST "localhost:9200/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary '
{ "index": { "_index": "logs", "_id": "abc-123" } }
{ "service": "payment-api", "level": "ERROR", "message": "Connection timeout" }
{ "index": { "_index": "logs", "_id": "def-456" } }
{ "service": "auth-service", "level": "INFO", "message": "Health check passed" }
'
```

## 5. The Query DSL (search syntax)

Searches are JSON sent to `POST /<index>/_search`. The two building blocks:

- **`query`** — full-text, **scored** by relevance (use `match`).
- **`filter`** — exact, **not scored**, cacheable, faster (use `term`, `range`).

```bash
# Full-text: messages containing "timeout" (relevance-ranked)
curl "localhost:9200/logs/_search?pretty" -H 'Content-Type: application/json' -d '{
  "query": { "match": { "message": "timeout" } }
}'

# Exact filter: all ERROR logs from payment-api (bool + filter)
curl "localhost:9200/logs/_search?pretty" -H 'Content-Type: application/json' -d '{
  "query": {
    "bool": {
      "filter": [
        { "term": { "level": "ERROR" } },
        { "term": { "service": "payment-api" } }
      ]
    }
  }
}'

# Combine: text search WITHIN a filtered subset, sorted by time, paginated
curl "localhost:9200/logs/_search?pretty" -H 'Content-Type: application/json' -d '{
  "query": {
    "bool": {
      "must":   [ { "match": { "message": "database" } } ],
      "filter": [ { "term": { "level": "ERROR" } },
                  { "range": { "timestamp": { "gte": "now-1h" } } } ]
    }
  },
  "sort": [ { "timestamp": "desc" } ],
  "from": 0, "size": 20
}'
```

Common query clauses:
- **`match`** — full-text on a `text` field (analyzed).
- **`term`** — exact match on a `keyword`/number/date field.
- **`terms`** — match any of a list of exact values.
- **`range`** — `gte`/`lte`/`gt`/`lt`, works on dates and numbers (`now-1h`, `now-1d`).
- **`bool`** — combine clauses: `must` (AND, scored), `filter` (AND, not scored),
  `should` (OR), `must_not` (NOT).

## 6. Aggregations (analytics / group-by)

Aggregations power dashboards. Set `size: 0` to get only the aggregation, no documents.

```bash
# Count logs per service (a "terms" bucket aggregation = GROUP BY service)
curl "localhost:9200/logs/_search?pretty" -H 'Content-Type: application/json' -d '{
  "size": 0,
  "aggs": {
    "by_service": { "terms": { "field": "service" } }
  }
}'

# Logs over time, bucketed per minute (date histogram)
curl "localhost:9200/logs/_search?pretty" -H 'Content-Type: application/json' -d '{
  "size": 0,
  "aggs": {
    "over_time": { "date_histogram": { "field": "timestamp", "fixed_interval": "1m" } }
  }
}'
```

## 7. How our code connects to & uses OpenSearch

We use the official **`opensearch-py`** client (see `consumer-python/consumer.py`).

### Connecting
```python
from opensearchpy import OpenSearch, helpers
client = OpenSearch("http://localhost:9200")   # security disabled locally
```

### Creating the index + mapping (our `ensure_index`)
```python
client.indices.exists("logs")          # → bool
client.indices.create("logs", body={
    "mappings": { "properties": {
        "service": {"type": "keyword"},
        "level":   {"type": "keyword"},
        "message": {"type": "text"},
        "timestamp": {"type": "date", "format": "epoch_millis"},
    }}
})
```
This is the Python equivalent of the `curl -X PUT` mapping in §4 — same JSON body.

### Bulk indexing (our hot path)
```python
actions = [
    {"_index": "logs", "_id": doc["id"], "_source": doc}   # one action per log
    for doc in batch
]
helpers.bulk(client, actions)          # one HTTP request for the whole batch
```
`helpers.bulk` is the client's wrapper around the `_bulk` REST endpoint from §4. Using
the log's `id` as `_id` makes re-indexing the same log an idempotent overwrite.

### Searching from code (same DSL as curl)
```python
resp = client.search(index="logs", body={
    "query": {"bool": {"filter": [{"term": {"level": "ERROR"}}]}}
})
hits = resp["hits"]["hits"]            # list of matching documents
```

## 8. Phase 2: vectors / kNN (optional)

With `USE_EMBEDDINGS=true`, `ensure_index` adds a vector field and enables kNN:
```python
"message_vector": {"type": "knn_vector", "dimension": 384}   # + settings: {"index": {"knn": true}}
```
Then a similarity search finds the nearest log vectors:
```bash
curl "localhost:9200/logs/_search" -H 'Content-Type: application/json' -d '{
  "size": 5,
  "query": { "knn": { "message_vector": { "vector": [/* 384 floats */], "k": 5 } } }
}'
```
Combining this with a `match`/`filter` clause gives **hybrid search**.

## 9. Where to see it visually
OpenSearch Dashboards at **http://localhost:5601** → create a data view on the `logs`
index (time field `timestamp`) → **Discover** to search, or **Visualize** to build the
aggregations from §6 as charts. See the README for click-by-click steps.


---

# Idempotency & Reliability — How the Pipeline Survives Failure

This is one of the most important system-design topics in the project. It answers:
*when things fail and work gets retried, how do we avoid corrupting the data?* The
answer is **idempotency**, and understanding where it lives (and where Kafka does vs.
doesn't help) is exactly the kind of thing a system-design interview probes.

---

## 1. The core problem: resilience is built on retries, and retries cause duplicates

In a distributed system, failure is normal — networks drop packets, processes crash,
consumers get reassigned. The universal way you build resilience is by **retrying**:
re-send the message, re-process the batch, resume from the last checkpoint.

But retries create a second problem: **duplicates**. If you process a message, crash
before recording that you did, then retry — you've now processed it twice. So the price
of "never lose data" (retries) is "might handle data twice." **Idempotency is the
technique that makes handling-twice harmless**, which is what lets you retry freely.

## 2. What "idempotent" means

An operation is **idempotent** if doing it many times has the same effect as doing it
once.

| Idempotent ✅ | Not idempotent ❌ |
|---|---|
| `x = 5` (always ends at 5) | `x = x + 1` (grows each time) |
| HTTP `PUT /user/42 {...}` | HTTP `POST /users {...}` (creates a new row each call) |
| "Upsert document with id=abc" | "Append a new document" |

The trick to making any pipeline idempotent: turn "create a new thing" into "set this
specific, identified thing" — i.e., key the write by a **stable, unique identifier** so
a repeat just overwrites instead of duplicating.

## 3. Delivery guarantees (the framing every system-design answer uses)

| Guarantee | How | Result |
|---|---|---|
| **At-most-once** | Commit offset *before* processing | Fast, but can **lose** messages on crash |
| **At-least-once** | Commit offset *after* processing | Never loses; may **duplicate** |
| **Exactly-once** | Transactions across broker + sink | Ideal, but complex/expensive/limited |

True end-to-end exactly-once is hard and usually requires distributed transactions that
your sink must support. So the **industry-standard pragmatic pattern** is:

> **At-least-once delivery + idempotent processing = "effectively-once".**

You accept that messages *might* arrive twice, and you make processing them twice
harmless. That's the pattern this pipeline uses.

## 4. The three places duplicates can appear in OUR pipeline

```
[1] Producer ──► Kafka       [2] Kafka ──► Consumer        [3] Consumer ──► OpenSearch
    (retry on a               (redelivery: rebalance,           (the write itself)
     lost ack)                 crash-before-commit)
```

1. **Producer → Kafka:** the producer sends a record, the broker writes it, but the ack
   is lost to a network blip; the producer retries and the *same record is written twice*
   in the Kafka log.
2. **Kafka → Consumer:** at-least-once redelivery. A consumer indexes a batch but crashes
   before `commit()`; on restart (or after a rebalance) it re-reads the same messages.
3. **Consumer → OpenSearch:** the indexing write happens again as a result of [2].

## 5. Does Kafka handle idempotency "naturally"? (the key question)

**Partially — and only for layer [1].**

- **Kafka's idempotent producer** (`enable.idempotence=true`) solves layer [1]. The
  producer gets a producer-id + per-partition sequence numbers; the broker uses them to
  **deduplicate retries**, so a retried send isn't written twice. It's a built-in toggle.
  *Caveat:* it requires `acks=all`. **Our `application.yml` currently sets `acks=1`, so
  producer idempotence is OFF** in our config (see §8 for the optional upgrade).
- **Kafka does NOT give you end-to-end exactly-once into an external store** like
  OpenSearch on its own. That would require Kafka **transactions** *and* a transactional
  sink that can commit atomically with the offset. OpenSearch isn't transactional in that
  way. So layers **[2] and [3] are our responsibility**, not Kafka's.

**Bottom line:** Kafka can dedupe its *own* producer retries, but the moment data leaves
Kafka for an external system, idempotency is on you.

## 6. How WE handle it (one technique covers all three layers)

We use the log's **`id` as the OpenSearch document `_id`** (in `consumer.py`):
```python
{"_index": "logs", "_id": doc["id"], "_source": doc}
```
Indexing a document with an existing `_id` **overwrites** it instead of creating a
duplicate (it's an upsert). Because the `id`:
- is a **stable, unique key** generated **once at the source** (a UUID in
  `LogSimulator`), and
- **travels inside the message** all the way to the sink,

...every duplicate — no matter which of the three layers it came from — carries the
*same* `id`, lands on the *same* `_id`, and harmlessly overwrites. **One technique
(deterministic sink key) neutralizes producer-retry dupes, redelivery dupes, and
rebalance dupes at once.**

This is the **"idempotent receiver"** / **"upsert by natural key"** pattern. We pushed
idempotency to the **sink** rather than trying to prevent duplicates everywhere upstream.

### Why the details matter (subtle but interview-critical)
- The key must be generated **at the source and carried through** — *not* regenerated in
  the consumer. If the consumer made a new id per processing attempt, retries would get
  *different* ids and duplicate. (This is also why a random UUID per `generateLog()` call
  is correct — decision #18.)
- The sink operation must be an **overwrite/upsert by that key** (OpenSearch `index` with
  an explicit `_id` is exactly that).

## 6b. What happens when the index write fails?

The at-least-once guarantee depends on the offset commit being **skipped** when indexing
fails — and it is. `helpers.bulk()` raises by default (`raise_on_error=True`) on:
- **OpenSearch unreachable** (transport/connection error), or
- **rejected documents** (`BulkIndexError`).

Because it raises *on that line*, the next line never runs:
```python
helpers.bulk(client, actions)   # ← raises
consumer.commit()               # ← never runs → offset NOT advanced
```
The offset stays put → Kafka redelivers those messages on the next run → **no loss**.

**Partial failures are safe too.** A bulk request can index some docs and fail others.
The successful ones are already in OpenSearch; since we don't commit, the restart
reprocesses the *whole* batch — and idempotent `_id` writes overwrite the already-indexed
docs harmlessly (another payoff of §6).

**Honest current limitation.** The loop wraps only per-message *parsing* in `try/except`,
not `bulk`/`commit`. So a bulk error **crashes the worker** instead of retrying. Data is
safe (restart resumes from the last commit), but it's not graceful. Hardened version:
```python
if actions:
    try:
        helpers.bulk(client, actions)
        consumer.commit()
    except Exception as e:
        log.warning("bulk failed; will retry batch", exc_info=e)
        time.sleep(backoff)   # no commit → same batch retried next loop
```
Tracked in open-questions.md.

## 7. Producer-side vs consumer-side — the system-design summary

| Layer | Who owns it | Mechanism | Status here |
|---|---|---|---|
| Producer retry dupes | Kafka (optional) | `enable.idempotence=true` (needs `acks=all`) | Off (we use `acks=1`); covered anyway by §6 |
| Redelivery / rebalance dupes | **You** | Idempotent processing | ✅ deterministic `_id` |
| Sink write dupes | **You** | Upsert by natural key | ✅ deterministic `_id` |

**How to say it in an interview:**
> "We use **at-least-once delivery** — commit offsets only after the batch is indexed —
> and make processing **idempotent** by using each log's source-generated id as the
> OpenSearch document id. That gives us **effectively-once** semantics without the cost
> of distributed transactions, and it makes the system safe to retry and to scale."

## 8. Optional upgrade: defense in depth

We could *also* enable Kafka's producer idempotence so duplicates never even enter the
log:
```yaml
producer:
  acks: all                 # required for idempotence
  properties:
    enable.idempotence: true
```
Trade-off: `acks=all` is slightly slower than `acks=1` (waits for replicas). Since our
sink-side idempotency already covers producer-retry dupes, this is **defense in depth**,
not strictly required. Tracked in open-questions.md.

## 9. Where idempotency fits in the bigger resiliency picture

**Is idempotency *the* resiliency concept?** It's one of several pillars — necessary but
not the whole story. Resilience in this pipeline is a *team* of concepts:

| Pillar | What it protects against | Where |
|---|---|---|
| **Durability / buffering** | Loss when a consumer is down | Kafka persists to disk |
| **At-least-once delivery** | Loss on crash mid-processing | Commit offset *after* indexing |
| **Idempotency** | **Corruption from the duplicates that at-least-once + retries create** | Deterministic `_id` |
| **Backpressure** | Overload / OOM under load | Synchronous pull→index→commit loop |
| **Dead-letter queue** | One poison message halting everything | `logs-dlq` |
| **Consumer groups / rebalancing** | A dead worker stalling the pipeline | Auto-failover to live members |

**The precise relationship:** retries/at-least-once give you *durability* but introduce
*duplicates*; **idempotency is the pillar that neutralizes those duplicates**, which is
what makes the durable, retryable, scalable design actually *correct*. So your intuition
is right — idempotency is central to error-handling and resiliency — but it works **with**
delivery semantics and the others, not instead of them. The cleanest mental model:

> **At-least-once gives you "never lose it." Idempotency gives you "never double it."
> Together they give you "effectively-once," which is what real systems ship.**

## 10. Idempotency is what makes *scaling* safe

This connects directly to scaling-and-backpressure.md:
every time you add or lose a consumer, Kafka **rebalances**, and rebalances cause
redelivery (layer [2]). Without idempotency, *every scaling event could corrupt data with
duplicates.* With it, you can add consumers freely. So idempotency isn't just an
error-handling nicety — it's a **prerequisite for horizontal scaling**.


---

# Scaling & Backpressure — Runtime Behavior Under Load

How the pipeline behaves when things get busy: how it self-regulates when a downstream
stage is slow (**backpressure**), and what happens when you add capacity (**scaling /
rebalancing**). Builds on kafka-explained.md and
consumer-explained.md.

---

# Part 1: Backpressure

## What it is

The term comes from plumbing: if water flows into a pipe faster than it drains, pressure
builds and **pushes back** against the source. In software, **backpressure** is any
mechanism that signals "slow down" upstream when a downstream stage can't keep up —
instead of piling work up until something breaks.

**Analogy — a supermarket checkout:** the cashier (consumer) scans items; the conveyor
belt (Kafka) holds groceries; customers (producer) add to the belt. If the cashier is
slow, the belt just holds more — nobody is forced to speed up and nothing falls on the
floor. The cashier's own pace limits how fast groceries leave the belt. That
self-limiting *is* backpressure.

## The problem it prevents

A naive worker with no backpressure grabs messages as fast as they arrive and queues
them internally to process "later." If the downstream (OpenSearch) slows, that internal
queue grows unbounded until the worker **runs out of memory and crashes** — losing the
unprocessed messages. Backpressure exists to prevent exactly this.

### "But isn't Kafka just the same queue? Won't it fill up too?"

Common and sharp question. The insight: the naive design has **two** queues, ours has
**one**.

- The naive worker pulls from Kafka *as fast as it can* and dumps everything into a
  **second, internal, in-memory queue inside its own process**. It's just moving the
  backlog from safe disk (Kafka) into unsafe RAM (its own heap).
- Our worker has **no second queue** — it pulls a batch, processes it, then pulls the
  next. The backlog only ever lives in **one** place: Kafka.

So why is the *same backlog* safe in Kafka but fatal in RAM?

| | Internal queue (worker RAM) | Kafka (disk) |
|---|---|---|
| Location | Inside the worker's heap | External broker, on disk |
| Capacity | A few GB (heap) | Hundreds of GB / TBs |
| When it fills | Seconds–minutes | Hours, days, or never |
| When full | **Hard crash** (OOM) | Gradual; oldest drops or producer throttles (configurable retention) |
| Durability | **Volatile** — crash = lost | **Durable** — survives restart |
| Recovery | Gone; reprocess from scratch | Restart, resume from committed offset |
| Visibility | Invisible until it crashes | Measurable as **consumer lag** — you see it coming |

Yes, Kafka *could* eventually fill its disk if the producer permanently outpaces the
consumer — but that's a different *class* of problem: slow, observable (lag climbs),
controllable (retention policies), and recoverable (durable). The RAM queue fails fast,
silently, and unrecoverably.

**The precise definition:** backpressure doesn't stop a backlog from existing — it
ensures the backlog accumulates in the place built to hold it safely (Kafka's disk)
instead of the place that crashes (the worker's memory). Our synchronous
pull → index → commit loop *is* the backpressure: pull rate is forced to equal process
rate, so no in-memory pile can form.

## The foundation: Kafka is "pull", not "push"

A Kafka consumer **pulls** messages when it's ready — Kafka never pushes them at you:

```python
batches = consumer.poll(timeout_ms=1000)   # the consumer ASKS for messages
```

The consumer decides *when* to ask. Unread messages sit safely on Kafka's disk until
asked for. A "push" system would need explicit "please slow down" signaling; Kafka's
pull model gives backpressure for free.

## The mechanism in our code

The consumer loop is **single-threaded and synchronous** — one thing at a time, in order:

```python
while running["on"]:
    batches = consumer.poll(timeout_ms=1000)   # 1. pull a batch from Kafka
    # ... parse into `actions` ...
    helpers.bulk(client, actions)              # 2. send to OpenSearch — BLOCKS here
    consumer.commit()                          # 3. mark them done
    # 4. loop back to step 1
```

`helpers.bulk(...)` is **blocking**: the program stops on that line until OpenSearch
responds. The loop cannot call `poll()` again until `bulk()` returns. So when OpenSearch
slows down:

```
OpenSearch slow → bulk() takes longer → loop waits → poll() not called
              → consumer isn't pulling → messages accumulate safely in Kafka
              → producer keeps running, unaffected
```

The consumer **literally cannot outrun OpenSearch** — it won't ask for the next batch
until it has finished the current one. The blocking call *is* the backpressure; there's
no throttle code. "Slowing down" is an emergent effect of waiting, not an instruction:
if `bulk()` goes from 50ms to 2s, the whole loop runs ~40× slower and pulls ~40× less
often.

## Why it's robust

- **No crash** — no unbounded internal queue, no memory blowup.
- **No loss** — the backlog lives on Kafka's disk, not fragile RAM.
- **Decoupled producer** — Java keeps producing; Kafka absorbs the difference.
- **Self-healing** — when OpenSearch recovers, `bulk()` speeds up and the consumer
  drains the backlog.

## Seeing it: consumer lag

The backlog is measurable as **consumer lag**:
```bash
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group log-indexers
```
`LAG = log-end-offset − current-offset` = messages waiting, unprocessed. Watch it climb
during a spike (or induced OpenSearch slowness), then shrink as the consumer catches up.
**That growing-then-shrinking number is backpressure made visible.**

## Caveat: `max.poll.interval.ms`

There's a floor on how slow you can go. If the loop takes longer than
`max.poll.interval.ms` (default **5 minutes**) between `poll()` calls, Kafka assumes the
consumer died and evicts it (triggering a rebalance — see Part 2). Healthy for normal
slowness; if a single batch could take >5 min to index, shrink `BATCH_SIZE` or raise the
timeout.

---

# Part 2: Scaling & Rebalancing

There are **three distinct scaling axes**, and they behave very differently:

| You add more… | Automatic? | The catch |
|---|---|---|
| **Consumers** (same group) | ✅ Yes — Kafka rebalances | Capped by partition count |
| **Partitions** (on a topic) | ⚠️ Half — you run a command, then consumers auto-adapt | Breaks key→partition mapping; can't undo |
| **Brokers** (Kafka servers) | ❌ No — data doesn't move itself | Needs manual partition reassignment |

## 2a. Adding consumers — automatic

The consumer group's **coordinator** watches membership. When a consumer joins, leaves,
or crashes, it triggers a **rebalance** — partitions are re-dealt among the live members.

**Analogy — dealing cards:** partitions are cards, consumers are players. Whenever a
player joins or leaves, the dealer collects all cards and re-deals them evenly. You never
say how — it just happens.

```
1 consumer, 3 partitions:        Add a 2nd consumer → automatic rebalance:
  consumer-A: [p0, p1, p2]         consumer-A: [p0, p1]
                                   consumer-B: [p2]
```

To scale processing: launch another `python consumer.py` with the same `group_id`. No
config, no coordination code.

**Hard limit — parallelism is capped by partition count.** One partition is consumed by
at most one group member at a time:
- 3 partitions + 3 consumers → each gets 1 (fully parallel) ✅
- 3 partitions + 5 consumers → 3 work, **2 sit idle** ⚠️

This is why our current **1 partition** is a limitation: 10 consumers, only 1 ever works.

## 2b. Rebalance issues

A rebalance isn't free:

- **Stop-the-world pause** — in the classic protocol, all consumers briefly pause while
  partitions are reassigned. (Newer **cooperative/incremental rebalancing** only moves
  the partitions that must move, avoiding the full pause.)
- **Duplicate processing** — if a consumer indexed a batch but crashed *before*
  `commit()`, the new owner re-reads from the last committed offset and re-processes.
  This is **at-least-once**. **Harmless for us** because we use the log `id` as the
  OpenSearch `_id`, so re-indexing overwrites identical data (idempotency, decision #14).
  A pipeline without idempotent writes would get duplicate records here.
- **Rebalance storms** — flaky/slow consumers that keep timing out
  (`max.poll.interval.ms`) drop and rejoin repeatedly, so the group spends more time
  re-dealing than working. Operational, not a code bug.

## 2c. Adding partitions — the tricky one

Partitions don't auto-scale; you run a command to increase the count:
```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --alter --topic raw-logs --partitions 3
```
Consumers then auto-pick-up the new partitions (via rebalance). Three gotchas:

1. **One-way: you can only increase, never decrease.** Reducing would require dropping or
   merging data, which Kafka won't do. You can't easily undo over-partitioning.
2. **Existing messages don't move.** Logs already in partition 0 stay there; only *new*
   messages use the new partition count. No automatic repartitioning of history.
3. **It breaks key-based ordering.** Partition is chosen by:
   ```
   partition = hash(key) % number_of_partitions
   ```
   Change the partition count and the same key may now map to a *different* partition, so
   its messages split across partitions — and order is only guaranteed *within* a
   partition. Historical per-key ordering breaks across the change.

**Does gotcha 3 bite us? No.** Our key is the log's **UUID** — unique per message — so we
never relied on per-key ordering. Adding partitions is **safe for this pipeline.** But if
the key were `user_id` (wanting each user's events in order), adding partitions would
scramble that ordering and require a planned migration. This is a real argument for
choosing partition count thoughtfully up front.

## 2d. Adding brokers — not automatic

Add a Kafka server and it starts **empty** — Kafka does not auto-move existing partitions
onto it. You run a **partition reassignment** (`kafka-reassign-partitions.sh`, or tooling
like Cruise Control) to spread data across brokers. The least automatic axis, and real
operational work at scale. (OpenSearch is similar: adding a node triggers shard
reallocation, not an instant free lunch.)

---

## What this means for our project

- **To scale processing:** the *mechanism* is automatic (more consumers → auto
  rebalance), but first bump `raw-logs` to ~3 partitions to unlock it (it's 1 now).
- **Rebalance-safe by design:** idempotent writes (UUID `_id`) neutralize the
  duplicate-processing risk.
- **Repartition-safe by design:** unique keys mean adding partitions won't scramble
  ordering.
- **The non-automatic part:** choosing the partition count wisely up front — you can't
  shrink it, and increasing it has ordering consequences for keyed data.

**The lesson:** Kafka automates the *redistribution* of work, but **capacity planning
(how many partitions) is a human decision with lasting consequences** — a classic
distributed-systems trade-off. See open-questions.md for the
"bump to 3 partitions" task this informs.

---

# Part 3: Operating safely — restarts, volumes & config changes

"Can I just stop Kafka, change settings, and start it again?" Mostly yes — with caveats.
The deciding factors are **(a) the data volume** and **(b) which setting you change.**

## What is stored where (the mental model)

- **OpenSearch is the durable system of record.** Once a log is indexed, it's safe
  regardless of what happens to Kafka. Kafka is **not** a backup of OpenSearch.
- **Kafka is a transient buffer** with time/size **retention** (default ~7 days), not
  permanent storage. Don't treat it as a second copy of your data.
- The only data **at risk** during Kafka maintenance is what's **in Kafka but not yet in
  OpenSearch** (unprocessed / in-flight). It's protected as long as you keep the volume.
- **Consumer offsets live in Kafka too** (the internal `__consumer_offsets` topic).
  Wiping Kafka's volume wipes the bookmarks as well.

So the "we have two stores, so we're fine" intuition is right *with one correction*: it's
safe **as long as you don't wipe the Kafka volume**, and the truly durable store is
**OpenSearch**, not Kafka.

## What survives a restart

| Action | Volume | Result |
|---|---|---|
| `docker compose restart kafka` | kept | topics, messages, offsets survive; consumer resumes from its committed offset |
| `docker compose down` then `up` | kept (`kafka-data`) | same — all data survives |
| `docker compose down -v` | **wiped** | all topics/messages/offsets **gone** (anything already in OpenSearch is still safe) |

## Which config changes are safe

| Change | How | Safe? |
|---|---|---|
| Add a topic / change retention | live or restart | ✅ easy |
| **Add partitions** to a topic | `kafka-topics --alter` (live, no restart) | ✅ one-way; reshuffles key→partition (Part 2c) |
| Heap, auto-create, most broker tunables | restart with volume kept | ✅ |
| **Cluster identity** — `NODE_ID`, cluster id, process roles, listeners | — | ⚠️ can corrupt / fail startup on an existing data dir |
| **Go from 1 broker to many** | not a value tweak — see below | ⚠️ real reconfiguration |

## Adding brokers is NOT a simple value change

Going multi-broker means: define multiple broker services (each a unique `NODE_ID`),
configure the controller quorum voters, and — crucially — existing topics stay at
**replication factor 1** until you run a **partition reassignment** to spread/replicate
them. New brokers start **empty**; data doesn't move itself (Part 2d). It's a planned
procedure, not "edit a number and restart."

## A safe maintenance procedure

1. (Optional) stop the producer to halt new inflow.
2. Let the consumer drain until **lag = 0** (nothing unprocessed) — check with
   `kafka-consumer-groups --describe`.
3. `docker compose down` **without `-v`** (or `docker compose restart kafka`).
4. Make only volume-safe config changes.
5. `docker compose up -d`.
6. Consumer reconnects and resumes from committed offsets.

**Golden rules:** keep the volume, drain to zero lag before maintenance when you can, and
treat **OpenSearch as the source of truth** with Kafka as a replaceable buffer.


---

# Multi-Broker Kafka — Dev Simulation & Production Scaling

We run **1 broker** (a single-node simulation). This doc covers two things:
- **Part A:** how to simulate a 3-broker cluster locally, and
- **Part B:** how broker scaling *actually* works in production — which is different,
  easier, and important to understand.

Background on the data model (brokers vs partitions, leaders/followers, replication) is
in kafka-reference.md §2b. Quick recap:

- **Broker** = a machine. **Partition** = a slice of a topic's data, stored *on* brokers.
- **More brokers** = more capacity + redundancy. **More partitions** = more parallelism.
- **Replication factor** = copies of each partition across brokers (RF ≤ #brokers).

---

# Part A: Simulating a 3-broker cluster locally

**Honest framing:** for our *combined* single-node KRaft setup (each node is broker **and**
controller), going multi-broker is **not** an "edit a number and restart" change — it
changes the controller quorum. The cleanest path for a dev sim is to stand up a **fresh
3-broker cluster.** Your real data is safe because **OpenSearch — the durable store — is
never touched.**

## Step 1: Drain first (so nothing is lost)
Let the consumer reach **lag = 0** so everything in Kafka is already in OpenSearch:
```bash
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group log-indexers
```

## Step 2: Replace the one `kafka` service with three
Each broker needs a **unique `NODE_ID`**, the **same `CLUSTER_ID`**, all three listed in
the **controller quorum**, and **two listeners** (one for inter-broker traffic inside
Docker, one for host apps). Broker 1 shown — brokers 2 and 3 differ only at the marked
lines:

```yaml
  kafka-1:
    image: apache/kafka:3.8.0
    container_name: kafka-1
    ports: [ "9092:9092" ]                         # broker-2: 9094 · broker-3: 9096
    environment:
      KAFKA_NODE_ID: 1                              # broker-2: 2 · broker-3: 3
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093
      KAFKA_LISTENERS: INTERNAL://0.0.0.0:19092,CONTROLLER://0.0.0.0:9093,EXTERNAL://0.0.0.0:9092
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka-1:19092,EXTERNAL://localhost:9092  # match host port
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      CLUSTER_ID: "5L6g3nShT-eMCtK--X86sw"           # SAME on all three
    volumes: [ "kafka1-data:/var/lib/kafka/data" ]   # broker-2: kafka2-data · broker-3: kafka3-data
```
Add `kafka1-data:`, `kafka2-data:`, `kafka3-data:` to the bottom `volumes:` block.

> **The fiddly part is the two-listener setup.** `INTERNAL` (advertised as `kafka-1:19092`)
> is used by the other brokers and Dashboards inside the Docker network; `EXTERNAL`
> (advertised as `localhost:909x`) is used by your host Java/Python apps. Each broker's
> `EXTERNAL` advertised port must match its mapped host port. This is the #1 thing to get
> right in multi-broker Docker.

## Step 3: Point your apps at all three brokers
- `producer-java/.../application.yml` → `bootstrap-servers: localhost:9092,localhost:9094,localhost:9096`
- Consumer → `BOOTSTRAP=localhost:9092,localhost:9094,localhost:9096`

(One broker is enough to bootstrap; listing all three is just more resilient.)

## Step 4: Bring it up and create the topic *with replication*
```bash
docker compose down            # NO -v — keeps your OpenSearch data
docker compose up -d

docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic raw-logs --partitions 3 --replication-factor 3
```

## Step 5: Verify it's a real cluster
```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic raw-logs
```
You'll see each partition listing a **Leader** on one broker and **Replicas/Isr** spanning
all three — proof the data is both **spread** (partitions) and **backed up** (replicas)
across the **machines** (brokers).

## What's safe vs discarded
- ✅ **OpenSearch data** — preserved (its volume is untouched).
- ✅ **In-flight Kafka data** — nothing, because you drained to lag 0 in Step 1.
- ❌ **Old single-node Kafka buffer + offsets** — discarded; fine since it was empty and the
  new cluster starts fresh offsets.

---

# Part B: How production scales brokers

Common (correct!) intuition: *"it's a cluster, so adding/removing brokers should be easy."*
In production it **is** — online and non-disruptive. The dev "rebuild" above is awkward
only because of two things specific to our toy setup, which production avoids.

## Why production isn't a rebuild

1. **Our nodes are "combined"** — broker **and** controller in one. Changing the node count
   changes the controller quorum (the cluster's metadata brain). That's the awkward part.
2. It's a throwaway dev sim, so rebuilding is simpler than doing it properly.

Production is **architected to avoid both.**

## The production architecture: separate controllers from brokers

```
CONTROLLERS (the "brain" — metadata only)     BROKERS (the "muscle" — hold the data)
  controller-1  ┐                               broker-1  ┐
  controller-2  ├─ small, FIXED set (3 or 5)    broker-2  ├─ scale these FREELY
  controller-3  ┘                               broker-3  │  (add / remove as needed)
                                                broker-4  ┘
```

- **Controllers** — a small, **fixed** set (3 or 5) managing cluster metadata. Rarely touched.
- **Brokers** — the data-carrying machines. **Scaled up and down freely**, and because
  adding one doesn't change the controller quorum, it doesn't disturb the cluster's brain.

This is why the "easy add/remove" intuition is **right in production** — brokers are
decoupled from controllers. Our single combined node is the special, awkward case.

## Adding a broker (online, no downtime)

```
1. Provision the new broker → unique node id, pointed at the cluster.
      → It JOINS automatically and is visible. But it's EMPTY (holds no partitions yet).

2. Reassign some partitions onto it (the deliberate step):
      kafka-reassign-partitions.sh --generate   # build a plan
      kafka-reassign-partitions.sh --execute     # move data (usually throttled)
      kafka-reassign-partitions.sh --verify      # confirm

3. (Optional) rebalance leadership so the new broker leads partitions, not just follows.
```
The cluster stays fully online — producers/consumers keep working, no restart of others.

## The one thing that isn't automatic: moving the data

Adding the *machine* is instant; moving *data* onto it is the deliberate part. Why Kafka
makes you trigger it:
- Rebalancing copies **gigabytes/terabytes** across the network.
- You want to control **when** and **how fast** (throttling), so it doesn't saturate the
  network and hurt live traffic.

So Kafka gives you a **controlled** operation, not an automatic stampede. The new broker
doesn't help until its share of partitions has moved over.

## Scaling down (the mirror image)
```
1. Reassign that broker's partitions OFF it → onto the remaining brokers.
2. Once it holds nothing, decommission it.
```
You can't just kill a broker holding data — you'd lose it (or drop into an
under-replicated state). Drain it first.

## What makes it feel "one-click" at scale

Real teams rarely run those CLI commands by hand:

| Tool | What it gives you |
|---|---|
| **Cruise Control** (LinkedIn, OSS) | Auto-generates & executes rebalancing plans; self-healing; anomaly detection |
| **Kubernetes operators** (Strimzi, Confluent Operator) | Declarative: change `replicas: 3 → 5`; the operator joins the broker *and* rebalances |
| **Managed Kafka** (AWS MSK, Confluent Cloud, Aiven) | A slider / "add broker" button; the provider handles provisioning + reassignment + throttling |

So the "just scale it" experience **does exist** — it's automation layered on top of the
controlled primitive, not baked into raw Kafka by default.

## Capacity-planning caveat

Adding brokers gives **room** (storage + load spreading) but doesn't increase a *topic's*
parallelism — that's still capped by **partition count**. Scaling out usually means *both*
"add brokers" (capacity) **and** "add partitions" (parallelism), then reassign.

---

## Key takeaways

- A Kafka cluster makes scaling **online and non-disruptive** — that's the "easy" part.
- It is **not automatic** by default, because moving data is expensive and Kafka wants you
  to control it (throttled reassignment).
- Production separates **fixed controllers** from **freely-scalable brokers**, which is why
  adding/removing brokers there is genuinely easy.
- **Cruise Control / operators / managed services** provide the one-click experience.
- Our single combined node is a *simulation* — the awkwardness of "rebuild to scale" is an
  artifact of that, not how real clusters work.


---

# Glossary

Quick-reference vocabulary for the project. Fuller explanations live in
kafka-explained.md and consumer-explained.md.

## Kafka

- **Broker** — the Kafka server itself (our `kafka` Docker container).
- **Cluster** — multiple brokers working together; partitions (and their replicas) are
  spread across them. We run a single-broker "cluster."
- **Replication factor (RF)** — how many copies of each partition exist, on different
  brokers. RF=1 (ours) = no redundancy.
- **Leader / follower** — per partition, the **leader** replica handles all reads/writes;
  **followers** passively copy it. If the leader's broker dies, a follower is promoted.
- **In-sync replica (ISR)** — a follower fully caught up with the leader; `acks=all` waits
  for the ISR. See kafka-reference.md §2b.
- **Controller** — in KRaft, the node(s) managing cluster metadata (the "brain"). Production
  keeps a small fixed set (3 or 5), separate from brokers, so brokers scale freely.
- **Partition reassignment** — the controlled operation (`kafka-reassign-partitions.sh`)
  that moves partitions/replicas across brokers — e.g., onto a newly added broker. Data
  doesn't move itself. See multi-broker-setup.md.
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
  re-processing. See scaling-and-backpressure.md.
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
  scaling-and-backpressure.md.
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
  idempotency-and-reliability.md.
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

## System design / scaling

- **Distributed system** — multiple components coordinating over a network to act as one.
  Defined by the architecture, **not** the database — a distributed system can run on a single
  DB. See scaling-playbook.md.
- **Stateless vs. stateful** — stateless components (app servers) hold no data between
  requests → trivial to distribute (just add copies). Stateful components (databases) hold
  data → hard to distribute → distributed *last*.
- **Throughput (RPS / QPS)** — requests/queries handled per second. The number that drives
  the whole design. See capacity-and-throughput.md.
- **Latency** — how long a single request takes (a different axis from throughput).
- **Sharding / partitioning** — splitting data across nodes so each holds a slice; the way you
  scale *writes*.
- **Read replica** — a copy of a database that serves reads only; scales reads, not writes.
- **CAP theorem** — under a network partition a distributed store must choose Consistency or
  Availability. Dynamo/Cassandra lean **AP** (available, eventually consistent).
- **Eventual consistency** — replicas converge to the same value over time, not instantly;
  acceptable for feeds/logs, not for balances.
- **Sharded/NoSQL store** — DynamoDB, Cassandra: sharding built in ("sharding-as-a-service");
  scales writes, trades away easy joins/transactions. See scaling-playbook.md.

## Java / Spring

- **Spring Boot** — Java framework that auto-configures components from `application.yml`
  and runs with one click.
- **Dependency injection (DI)** — Spring constructs objects (like `KafkaTemplate`) and
  hands them to your classes via the constructor, instead of you `new`-ing them.
- **`@SpringBootApplication`** — marks the main entry-point class.
- **`ApplicationRunner`** — a Spring hook that runs once on startup (our producer loop).
- **JDK / LTS** — Java Development Kit; LTS = Long-Term Support release (we use JDK 21).
