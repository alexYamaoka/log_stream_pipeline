# The Python Consumer & OpenSearch, Explained From Zero

The read side of the pipeline. Covers consumer groups, offsets/commits, batching,
the dead-letter queue, and how logs land in OpenSearch. Pairs with
[kafka-explained.md](kafka-explained.md).

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
