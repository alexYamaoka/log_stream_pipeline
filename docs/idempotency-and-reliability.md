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
Tracked in [open-questions.md](open-questions.md).

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
not strictly required. Tracked in [open-questions.md](open-questions.md).

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

This connects directly to [scaling-and-backpressure.md](scaling-and-backpressure.md):
every time you add or lose a consumer, Kafka **rebalances**, and rebalances cause
redelivery (layer [2]). Without idempotency, *every scaling event could corrupt data with
duplicates.* With it, you can add consumers freely. So idempotency isn't just an
error-handling nicety — it's a **prerequisite for horizontal scaling**.
