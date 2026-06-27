# Multi-Broker Kafka — Dev Simulation & Production Scaling

We run **1 broker** (a single-node simulation). This doc covers two things:
- **Part A:** how to simulate a 3-broker cluster locally, and
- **Part B:** how broker scaling *actually* works in production — which is different,
  easier, and important to understand.

Background on the data model (brokers vs partitions, leaders/followers, replication) is
in [kafka-reference.md](kafka-reference.md) §2b. Quick recap:

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
