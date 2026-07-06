# System Design Notes

Notes on estimating load and scaling reads/writes/storage.
General knowledge (not project-specific), with this project as a running example.

**Companion docs:** `PROJECT.md` (the project) · `DISTRIBUTED-SYSTEMS.md` (how the tech works).

## Contents
1. **Capacity & Throughput Estimation** — RPS + storage estimation, scale tiers, this project's numbers
2. **Scaling Playbook** — reads vs writes, sharding, choosing a database, distributed != distributed DB

---

# Capacity & Throughput Estimation

How to estimate load (requests/sec), why it drives the whole design, and where the design
fundamentally changes. Pairs with
scaling-playbook.md (what to *do* with the number).

---

## 1. What "requests per second" means

**RPS / QPS / throughput** = how much work the system handles per unit time (for this
project: logs/sec). Distinct from **latency** (how long *one* request takes). Two numbers
matter:
- **Average** load (steady state).
- **Peak** load (the spike) — usually **2–10× average**. *You design for peak.*

## 2. Why it matters — it drives the whole design

The RPS number decides:
- **One machine, or distributed?** The biggest fork. Below a threshold, one server + one DB
  is the *correct, simplest* answer. Above it → queues, sharding, replicas, caching.
- **Where's the bottleneck?** Components have wildly different ceilings (a cache does 100× a
  SQL DB). The number tells you which one breaks first.
- **Cost.** 1M RPS might mean hundreds of servers.

What matters is **justifying design with math**, not memorizing exact numbers.

## 3. Back-of-envelope: the "drop 5 zeros" trick

A day has ~86,400 seconds → **round to 100,000.** Converting per-day → per-second is then
just **erase 5 zeros** (because 100,000 = 1 with 5 zeros; the zeros cancel in the division):

```
   100,000,000  per day        ← 8 zeros
   ───────────
       100,000  sec/day        ← 5 zeros
   cross off 5 →  1,000  per second
```

| Per day | drop 5 zeros | Per second |
|---|---|---|
| 1,000,000 (1M) | | **10 /sec** |
| 100,000,000 (100M) | | **1,000 /sec** |
| 1,000,000,000 (1B) | | **10,000 /sec** |

**Round first** so the number ends in ≥5 zeros; back-of-envelope wants the *order of
magnitude*, not precision. Keep the front digits: 400,000,000/day → **4,000/sec**.

### The whole estimate is 2 steps
```
1. total/day = users × actions per user per day     (one multiply)
2. per sec   = total/day ÷ 100,000 (drop 5 zeros), then ×3 for peak
```
Example: 10M users × 10 actions = 100M/day → drop 5 zeros → 1,000/sec → ×3 peak → **~3,000/sec**.

### Cheat sheet (memorize only this)
| Thing | Value |
|---|---|
| Seconds in a day | ~100,000 (drop 5 zeros) |
| 1 million/day | ~10/sec |
| 1 billion/day | ~10,000/sec |
| Peak multiplier | ×2–3 (×10 if spiky) |
| Storage/day | requests/day × size per request |
| **Key threshold** | **~10K writes/sec → "need distributed / sharding / a queue"** |

### Storage estimation (the easy way)

**Three throughput dimensions** exist — prioritize: **(1) request rate (RPS)** always;
**(2) storage growth** often; **(3) bandwidth (MB/s)** only for media-heavy systems
(= rate × payload size).

**Unit ladder** (×1000 = one unit up = 3 zeros):

| Unit | Bytes | Zeros |
|---|---|---|
| KB | 1,000 | 3 |
| MB | 1,000,000 | 6 |
| GB | 1,000,000,000 | 9 |
| TB | 10¹² | 12 |
| PB | 10¹⁵ | 15 |

Bytes → a unit = **drop that many zeros** (GB = drop 9, TB = drop 12).

**Default item sizes:** log / tweet / JSON / DB row = **1 KB** · photo = **1 MB** ·
1 min video ≈ **30 MB** · movie ≈ **1 GB**.

**Recipe** (stay in the unit — don't touch bytes):
```
1. writes/sec
2. × 100,000            → writes/day (add 5 zeros)
3. × size per item      → in that unit
4. walk ÷1000 per step  → GB/day (or TB)
   shortcut @1KB/item:  GB/day = writes/day with 6 zeros dropped
   then ×365 for /year, ×replication factor
```
Example: 5,000 logs/s → 500,000,000 logs/day × 1 KB → drop 6 zeros → **500 GB/day** ≈ 180 TB/yr.

**Storage tiers:**

| Storage growth | What you do |
|---|---|
| GB/day (< ~1 TB total) | single disk / DB fine |
| TB/day | distributed storage, **retention**, **tiering** (hot/warm/cold), compression, object storage (S3) |
| PB total | big-data infra (data lakes, columnar) |

## 4. Scale tiers — where the design changes

| Throughput | What it takes | Design change |
|---|---|---|
| **< ~1K RPS** | one server + one DB | nothing special; don't over-engineer |
| **~1K–10K** | one strong server + cache + read replicas | add cache, replicas |
| **~10K–100K** | horizontal scaling; sharding begins; queues | **1st inflection** — past a single DB's writes |
| **~100K–1M+** | fully distributed, partitioned, multi-region | **2nd inflection** — Kafka-scale infra mandatory |

Two anchors: **~10K writes/sec** = "single DB stops keeping up → shard + queue" (most
important line). **~1M RPS** = big-tech infra.

## 5. Worked examples: S / M / L / XL

Assumptions (only DAU changes): **100 reads/day + 10 writes/day** per user (10:1),
**peak = 3× average.** Watch the drop-5-zeros trick.

**🟢 Small — 10K DAU (internal tool / early startup)**
```
Reads:  10K × 100 = 1,000,000/day → drop 5 zeros → 10/sec → ×3 = ~30/sec peak
Writes: 10K × 10  =   100,000/day → drop 5 zeros →  1/sec → ×3 =  ~3/sec peak
```
→ **One DB, no cache, no queue.** Anything more is over-engineering.

**🟡 Medium — 1M DAU (growing product)**
```
Reads:  1M × 100 = 100,000,000/day → 1,000/sec → ×3 = ~3,000/sec peak
Writes: 1M × 10  =  10,000,000/day →   100/sec → ×3 =   ~300/sec peak
```
→ Writes trivial for one primary. Reads climbing → **add a Redis cache** (+ maybe a read
replica). Still one primary, no sharding, no queue.

**🟠 Large — 50M DAU (popular app) — the inflection point**
```
Reads:  50M × 100 = 5,000,000,000/day → 50,000/sec → ×3 = ~150,000/sec peak
Writes: 50M × 10  =   500,000,000/day →  5,000/sec → ×3 =  ~15,000/sec peak
```
→ Reads ~150K/s: **cache cluster + read replicas + CDN.** Writes ~5K sustained / ~15K peak
at the single-primary edge → **queue for bursts + begin sharding** (distributed SQL / NoSQL).

**🔴 XL — 500M DAU (Twitter-scale) — fully distributed**
```
Reads:  500M × 100 = 50,000,000,000/day → 500,000/sec → ×3 = ~1,500,000/sec peak
Writes: 500M × 10  =  5,000,000,000/day →  50,000/sec → ×3 =   ~150,000/sec peak
```
→ Reads ~1.5M/s: CDN + multi-layer cache + many replicas, likely multi-region. Writes
~150K/s: **sharded NoSQL / heavily-sharded SQL + Kafka.** No single primary.

**Summary**

| Scale | Reads/s (peak) | Writes/s (peak) | Architecture |
|---|---|---|---|
| Small 10K DAU | ~30 | ~3 | 1 DB, nothing else |
| Medium 1M DAU | ~3K | ~300 | 1 DB **+ cache** |
| Large 50M DAU | ~150K | ~15K | cache + replicas + CDN; **queue + shard writes** |
| XL 500M DAU | ~1.5M | ~150K | fully distributed: CDN + cache + **sharded/NoSQL + Kafka** |

The move is driven by **where each number lands**, not a fixed template — and reads/writes
are handled differently (cache reads, shard writes).

## 6. Component ceilings (order of magnitude, single node)

| Component | Rough ceiling |
|---|---|
| Web/app server (simple req) | ~10K–50K RPS |
| **Relational DB (writes)** | **~1K–10K/sec** ← usual bottleneck |
| Relational DB (cached reads) | ~10K–50K/sec |
| Redis / cache | ~100K+ ops/sec |
| **Kafka broker** | **~hundreds of thousands msgs/sec** |
| OpenSearch/Elasticsearch node (bulk) | ~10K–50K docs/sec |

Kafka & Redis are 10–100× a SQL DB — which is *why* high-throughput designs lean on them.

## 7. Reads vs. writes scale differently

- **Reads** are cheap: cache + read replicas. Ceiling is high and easy to raise.
- **Writes** are the hard constraint: every write hits the authoritative copy; scaling means
  sharding.

So split any RPS into reads vs writes and **worry mostly about the writes.** Details in
scaling-playbook.md.

## 8. This project's throughput (bottleneck analysis)

Find the slowest stage — that's the real throughput:

```
Java producer → Kafka → Python consumer → OpenSearch
  ~100K+/s      ~100K+/s    ~few K/s          ~10K–50K/s
   (fast)        (fast)     ← BOTTLENECK       (medium)
```

- **Producer → Kafka:** tens of thousands to 100K+/s (tiny JSON, batched, snappy). Spike mode
  prints the measured `logs/sec`.
- **Kafka:** not the bottleneck — hundreds of thousands/s on one broker.
- **Consumer → OpenSearch:** the ceiling — single-threaded Python, pure-Python Kafka client,
  synchronous bulk of 200 → realistically **~2K–10K logs/sec** on a laptop.

**Honest estimate:** end-to-end sustains **~a few thousand logs/sec**; the producer can burst
faster and Kafka absorbs the difference — which is the whole point of the decoupling.

## 9. Scaling this project further (walk the bottleneck = consumer→OpenSearch)

- **Parallelize the consumer:** bump `raw-logs` to N partitions + run N consumers (same group)
  → near-linear speedup. Swap `kafka-python-ng` for `confluent-kafka` (librdkafka, faster).
- **Tune OpenSearch writes:** bigger bulks; `refresh_interval: -1` during heavy ingest;
  disable replicas during bulk loads; more shards / nodes.
- **More infra:** more Kafka partitions/brokers (ingest headroom); more OpenSearch nodes
  (indexing ceiling).

## 10. Estimation mindset

- You need **order-of-magnitude estimates + the method**, not exact numbers.
- **State assumptions** (DAU, actions/user, peak ×) out loud.
- Use the number to **justify each component** ("50K writes > one DB, so queue + shard").
- Always **name the bottleneck** — that's the senior move.


---

# Scaling Playbook — Reads, Writes & Choosing a Database

General system-design notes (not specific to this project, but the project is a working
example — see the end). Covers how to scale reads vs. writes, the database-choice
decision framework, and the trade-offs.

Pairs with capacity-and-throughput.md (how to *estimate* the
load) — this doc is what to *do* with that number.

---

## Distributed system ≠ distributed database

A **distributed system** is defined by multiple components coordinating **over a network** —
it says nothing about the database. So you can (and usually do) run a **distributed system on
a single database.**

- **Stateless app servers** (behind a load balancer) are trivial to distribute — just run
  more identical copies. This alone already makes the system distributed.
- **Databases are stateful** → hard to distribute (the sharding pain in §5). So the DB stays
  centralized as long as possible and is distributed **last**, only when write volume forces it.

```
        ┌─ app 1 ─┐
users ─►│  app 2   ├─► ONE MySQL       ← still a distributed system;
   LB   └─ app 3 ─┘   (single primary)    the distribution is in the app tier
```

"MySQL cluster" is a spectrum, and a distributed system can sit on any rung:

| DB setup | DB itself distributed? | Scales |
|---|---|---|
| 1 MySQL instance | No (centralized) | one node |
| MySQL + read replicas | Partially (1 write primary) | reads + HA, **not** writes |
| Sharded MySQL (Vitess) | Yes (distributed writes) | reads + writes |

**This project** is a distributed system (Java → Kafka → Python → OpenSearch, all separate
processes over the network) with a **single-node** OpenSearch — i.e., a distributed *system*
with a non-distributed *database*. The distribution is in the pipeline, not the storage.

## 1. The golden rule

**Reads and writes scale completely differently. Writes are the hard problem.**

- **Reads** are cheap to scale: cache them, replicate them.
- **Writes** are expensive: every write must land on the authoritative copy. Scaling writes
  means **sharding** — genuinely distributing the data.

Rough single-node ceilings (order of magnitude, fuzzy — depends on hardware):

| | Comfortable on one node | Scale past it with… | Difficulty |
|---|---|---|---|
| **Reads** | ~10K–50K/sec (100K+ with cache) | cache + read replicas | 🙂 easy |
| **Writes** | ~1K–10K/sec | sharding / partitioning | 😣 hard |

> **10K writes/sec is borderline, not an automatic trigger.** A tuned single Postgres with
> batching can sometimes hit ~10–20K simple writes/sec. It's ~**100K+** where you *clearly*
> must go distributed.

### Scaling tiers at a glance

**Reads** (easy — cache + replicas):

| Reads/sec | Method |
|---|---|
| < ~1K | single DB, no cache |
| ~1K–10K | + cache (Redis), maybe a read replica |
| ~10K–100K | cache cluster + replicas + CDN |
| > ~100K | heavy caching, many replicas, CDN, geo |

**Writes** (hard — shard):

| Writes/sec | Method |
|---|---|
| < ~1K | single DB |
| ~1K–10K | one strong primary; queue if bursty; batch |
| ~10K–50K | shard (distributed SQL / NoSQL) + queue |
| > ~50K | many write nodes (NoSQL / heavily-sharded SQL) |

**Storage growth:**

| Growth | Method |
|---|---|
| GB/day | single disk / DB |
| TB/day | distributed storage, retention, tiering, S3 |
| PB total | big-data infra |

**Anchor:** **~10K writes/sec = the edge of a single primary** → the "now it's distributed"
line. (Reads hit "add a cache" around a few K/sec, but that's cheap; the *write* number forces
architectural change.)

## 2. Scaling reads — the easy playbook (in order)

1. **Cache** (Redis / Memcached) in front of the DB — serve hot reads from memory. Biggest,
   cheapest win.
2. **Read replicas** — copies of the DB that serve reads only; the primary handles writes.
3. **CDN** — cache static content at the edge, near users.

Most systems are **read-heavy** (often 100:1 or 1000:1 reads:writes), so a system doing
200K reads/sec might only do ~2K writes/sec → cache the reads and the writes fit on one DB.

## 3. Scaling writes — the harder playbook (in order)

1. **Vertical scale** (bigger box) — cheapest first move, buys time.
2. **Queue the writes** — put a message queue (**Kafka**) in front to absorb bursts and
   write to the store at a steady rate. A bursty 10K/sec can become a steady, single-DB-sized
   rate. *(This is this project's pattern.)*
3. **Shard / partition** — split the data across nodes:
   - **Distributed SQL** — Citus (Postgres), Vitess (MySQL), CockroachDB, Spanner,
     Aurora Limitless. Keeps SQL/transactions, adds sharding.
   - **Natively-sharded NoSQL** — DynamoDB, Cassandra. Sharding is built in.

### Which steps are universal vs. relational-specific
- **Vertical scaling** and **queuing** are **universal** — they sit in front of *any*
  database, relational or NoSQL.
- **Sharding** is where they differ: for **relational** it's a deliberate *later* step you
  add; for **NoSQL** (Dynamo/Cassandra) it's *built in from day one* — you never "then
  shard," you scale by adding nodes.
- So "vertical → queue → shard" is the **evolution story of a system that *began*
  relational.** A natively-sharded store starts at the finish line for sharding.

### The queue's limit (important)
A queue absorbs **bursts** and lets a slow sink **drain steadily** — but it does **not raise
the sink's sustained ceiling.** If the *sustained* write rate exceeds sink capacity, the
backlog grows forever. The queue buys **time and smoothing, not capacity**; a true sustained
shortfall still forces sharding / adding nodes. True for any sink (Postgres or Cassandra).

### Two tiers to scale: processing vs. storage
In a queue-based pipeline (like this project) there are **two separate** things to scale, with
**two separate bottlenecks**:
- **Processing tier** (the consumers) — how fast you pull from the queue and *attempt* writes.
  Scale with **more partitions + more consumers**. Stateless → easy.
- **Storage tier** (the sink DB) — how fast the datastore *accepts* writes. Scale by
  **sharding / adding nodes**. Stateful → the ultimate write ceiling.

Adding consumers when the **DB** is the bottleneck does nothing (they just stall on the DB);
adding DB nodes when the **consumers** are the bottleneck does nothing (the DB sits idle).
**Scale the saturated tier, and keep them balanced.** Diagnose with **consumer lag** (consumers
behind) vs. **DB write latency / utilization** (DB behind).

## 4. The "two kinds of cluster" trap (important!)

People say "cluster" for two very different things:

**Type 1 — Replication cluster (RDS Postgres/MySQL, Aurora):** scales **reads + HA, NOT writes.**
```
        ┌──► read replica  (reads)
writes ─┤
        └──► PRIMARY ──────► read replica  (reads)
             ▲  ALL writes funnel to ONE primary
```
Adding replicas does nothing for write throughput. To scale Postgres *writes* you need a
sharding layer (Citus / Aurora Limitless), not just replicas.

**Type 2 — Natively-sharded store (Cassandra, DynamoDB):** scales **writes.**
```
writes ─┬──► Node A (partition keys 0–33)   ← each node accepts writes
        ├──► Node B (partition keys 34–66)     for its own slice; no single
        └──► Node C (partition keys 67–99)     primary → add nodes = more writes
```

**Key insight:** DynamoDB/Cassandra are **"sharding-as-a-service"** — they *are* the sharding,
just managed and automatic.

## 5. How sharding a relational DB actually works

When vertical scaling + queuing aren't enough, sharding Postgres is a **data migration + an
app-layer change**, not a config toggle. That's why it's the last resort.

**What it means:** split rows across multiple independent Postgres primaries, each owning a
subset chosen by a **shard key**:
```
Before:  [ ONE Postgres ]   ← all rows, all writes
After:   [ Postgres A ] key % 3 == 0
         [ Postgres B ] key % 3 == 1     → ~3× write capacity
         [ Postgres C ] key % 3 == 2
```

**Three hard parts:**
1. **Choose a shard key** — the column you split on (`user_id`, `tenant_id`). The most
   consequential, hardest-to-change decision. Needs even distribution (no hot shards) and
   should appear in most queries.
2. **Move all existing data** — every row relocated to its target shard. *This is the
   "migrate the whole database" part — yes, literally.*
3. **Make the app shard-aware** — every query must route to the right shard; queries *without*
   the shard key become "scatter-gather" (hit all shards); cross-shard JOINs/transactions get
   hard or impossible.

**The zero-downtime migration pattern (dual-write + backfill + cutover):**
```
1. Provision N shard instances.
2. DUAL-WRITE: app writes to BOTH the old DB and the new shards.
3. BACKFILL: copy historical rows to their target shard (background job).
4. VERIFY: reconcile old vs new (counts, checksums).
5. SHIFT READS gradually (canary a %).
6. CUT OVER: stop writing to old; decommission it.
```
For a large DB this is often a multi-week project — hence "last resort."

**Re-sharding later is worse.** With naive `hash(key) % N`, adding a shard (N→N+1) remaps
almost every key → you move nearly all the data again. Mitigations:
- **Consistent hashing** — only a fraction of keys move when you add a node.
- **Pre-split into many logical shards** — e.g., 1,024 logical shards on 4 machines; add
  capacity by moving whole logical shards, no per-key recompute. (Instagram/Notion do this.)

**You rarely hand-roll it** — most teams adopt tooling:

| Option | What it does |
|---|---|
| **Citus** (Postgres extension) | Declare a distribution column; shards + routes automatically; `rebalance` to add nodes |
| **Vitess** (MySQL) | Same idea; powers YouTube, Slack |
| **CockroachDB / Spanner / Yugabyte** | Distributed SQL — auto-shards under one SQL interface (avoid manual sharding by choosing these) |
| **Aurora Limitless** (AWS) | Managed sharded Postgres-compatible |

**Recurring theme:** the shard key is the *same concept* as Kafka's partition key and
DynamoDB's partition key — always "which key decides which node holds this data." Choosing it
well (even distribution, present in your access patterns, hard to change later) is the core
distributed-systems skill, and the "re-sharding is painful" pain mirrors Kafka's one-way
partition-count decision.

### Scaling a NoSQL sink: adding nodes ≠ a migration
NoSQL stores (DynamoDB, Cassandra) are **pre-sharded**, so scaling writes = **add nodes**, not a
migration. **Consistent hashing** is why it's cheap: adding a node moves only ~**1/N** of the
data, not everything (unlike naive `hash % N`, which remaps almost every key).

| Sink | Scale writes = | How heavy |
|---|---|---|
| Postgres (retrofit sharding) | dual-write + backfill + cutover **migration** | 😣 heavy (multi-week) |
| Cassandra (self-managed) | add node → auto-joins ring, streams ~1/N of data, online | 🙂 routine ops |
| DynamoDB (managed) | change capacity / on-demand — AWS auto-splits partitions | 😎 trivial, invisible |

**Node vs. shard:** a *node* is a machine; a *shard/partition* is a data slice. Adding a node
redistributes existing shards onto more machines; managed systems also split shards
automatically. Our OpenSearch sink is Cassandra-like — adding a node triggers automatic shard
reallocation.

## 6. Choosing a database — the decision framework

Don't answer "which DB?" with the raw number. Use **access pattern + consistency + whether
scale is known/sustained.**

```
Need joins / multi-row transactions / ad-hoc queries / strong consistency?
   └─ YES → Relational (Postgres). If writes are high, SHARD it (Citus/CockroachDB).
   └─ NO  → Simple key-based access + eventual consistency OK + high writes?
              └─ YES → DynamoDB / Cassandra (natively sharded)

Is the scale UNCERTAIN / early-stage?
   └─ Start with Postgres (+ cache, replicas, queue). Know your migration trigger.
      (Don't over-engineer — Postgres handles more than people expect.)

Are the writes BURSTY rather than sustained?
   └─ Queue (Kafka) in front → steady write rate → a single/replicated Postgres may suffice.
```

### "Start with Postgres and migrate to Dynamo later" — good or bad?
- Good instinct **when scale is uncertain** (YAGNI). But **don't state "I'll migrate later" as
  the plan** — DB migrations are brutally expensive and it sounds naive. Frame it as: *"start
  with Postgres because requirements are uncertain; my trigger to move write-heavy data off it
  is X."*
- If the requirement **clearly states** high sustained writes **and** a key-value access
  pattern, designing a Postgres system you know will fail is the *wrong* answer — pick
  DynamoDB/Cassandra up front.

## 7. The trade-offs you accept when you shard / go NoSQL

- **Hard cross-shard joins & transactions** — data on different nodes can't be joined or
  updated atomically easily.
- **You must pick a good partition key** — a bad one creates "hot shards."
- **Often eventual consistency** — Dynamo/Cassandra lean **AP** (CAP theorem): available but
  not always immediately consistent.

So: use relational as long as you can (transactions, joins, one box is simpler); move
write-heavy data to a sharded/NoSQL store only when volume forces you.

## 8. Managed AWS options (quick reference)

| Service | Type | Scales | Good for |
|---|---|---|---|
| RDS Postgres/MySQL | Relational + replicas | reads + HA | transactions, joins, moderate writes |
| Aurora | Relational, cloud-native | reads (many replicas), 1 writer | relational at scale, read-heavy |
| Aurora Limitless / Citus | Distributed SQL | reads **+ writes** (sharded) | relational that must scale writes |
| **DynamoDB** | Managed NoSQL (KV) | reads **+ writes** (auto-sharded) | high writes, key-based access |
| Keyspaces (Cassandra) | Managed wide-column | reads **+ writes** | write-heavy, time-series/logs |
| ElastiCache (Redis) | In-memory cache | reads | hot-read caching |

## 9. A structured way to decide

```
1. Clarify: scale (sustained vs peak), access pattern, consistency needs.
2. State the number + implication: "~10K sustained writes is at the edge of one primary."
3. Choose by ACCESS PATTERN, not the number:
     key-value + eventual-consistency-ok → DynamoDB / Cassandra
     relational + transactions           → sharded Postgres (Citus)
     bursty                              → queue in front, single/replicated Postgres
4. Name the trade-off you're accepting (joins/consistency vs write-scale).
```
**"It depends, here's the decision tree"** is stronger than dogmatic "always Postgres" or
"always Dynamo at 10K."

## 10. This project is a live example

You've already used this playbook:
- **Kafka in front of OpenSearch** = write playbook step 2 (queue absorbs write bursts so the
  slow sink isn't overwhelmed).
- **Kafka partitions** = sharding the stream by key.
- **OpenSearch shards** its index the same way Cassandra shards data.

So the pipeline is "put a queue in front, then shard the sink" — the write-scaling playbook in
miniature.
