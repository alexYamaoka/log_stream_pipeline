# Capacity & Throughput Estimation

System-design interview prep: how to estimate load (requests/sec), why it drives the whole
design, and where the design fundamentally changes. Pairs with
[scaling-playbook.md](scaling-playbook.md) (what to *do* with the number).

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

Interviewers grade whether you **justify design with math**, not whether you memorized
numbers.

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
[scaling-playbook.md](scaling-playbook.md).

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
- **With `USE_EMBEDDINGS=true`:** collapses to **~hundreds/sec** (Ollama CPU embeddings). But
  embeddings are OFF by default, so this doesn't apply to the current setup.

**Honest estimate:** end-to-end sustains **~a few thousand logs/sec** (embeddings off); the
producer can burst faster and Kafka absorbs the difference — which is the whole point of the
decoupling.

## 9. Scaling this project further (walk the bottleneck = consumer→OpenSearch)

- **Parallelize the consumer:** bump `raw-logs` to N partitions + run N consumers (same group)
  → near-linear speedup. Swap `kafka-python-ng` for `confluent-kafka` (librdkafka, faster).
- **Tune OpenSearch writes:** bigger bulks; `refresh_interval: -1` during heavy ingest;
  disable replicas during bulk loads; more shards / nodes.
- **More infra:** more Kafka partitions/brokers (ingest headroom); more OpenSearch nodes
  (indexing ceiling).
- **If embeddings are on:** batch them, use a GPU, or a dedicated embedding service.

## 10. Interview mindset

- You need **order-of-magnitude estimates + the method**, not exact numbers.
- **State assumptions** (DAU, actions/user, peak ×) out loud.
- Use the number to **justify each component** ("50K writes > one DB, so queue + shard").
- Always **name the bottleneck** — that's the senior move.
