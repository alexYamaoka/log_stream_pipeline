# Scaling & Backpressure — Runtime Behavior Under Load

How the pipeline behaves when things get busy: how it self-regulates when a downstream
stage is slow (**backpressure**), and what happens when you add capacity (**scaling /
rebalancing**). Builds on [kafka-explained.md](kafka-explained.md) and
[consumer-explained.md](consumer-explained.md).

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
distributed-systems trade-off. See [open-questions.md](open-questions.md) for the
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
