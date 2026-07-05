# Scaling Playbook — Reads, Writes & Choosing a Database

General system-design interview prep (not specific to this project, but the project is a
working example — see the end). Covers how to scale reads vs. writes, the database-choice
decision framework, and the trade-offs.

Pairs with [capacity-and-throughput.md](capacity-and-throughput.md) (how to *estimate* the
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

## 9. The interview script

```
1. Clarify: scale (sustained vs peak), access pattern, consistency needs.
2. State the number + implication: "~10K sustained writes is at the edge of one primary."
3. Choose by ACCESS PATTERN, not the number:
     key-value + eventual-consistency-ok → DynamoDB / Cassandra
     relational + transactions           → sharded Postgres (Citus)
     bursty                              → queue in front, single/replicated Postgres
4. Name the trade-off you're accepting (joins/consistency vs write-scale).
```
Interviewers reward **"it depends, here's my decision tree"** over dogmatic "always Postgres"
or "always Dynamo at 10K."

## 10. This project is a live example

You've already used this playbook:
- **Kafka in front of OpenSearch** = write playbook step 2 (queue absorbs write bursts so the
  slow sink isn't overwhelmed).
- **Kafka partitions** = sharding the stream by key.
- **OpenSearch shards** its index the same way Cassandra shards data.

So the pipeline is "put a queue in front, then shard the sink" — the write-scaling playbook in
miniature.
