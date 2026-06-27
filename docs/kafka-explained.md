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

See [consumer-explained.md](consumer-explained.md) for the read side.
