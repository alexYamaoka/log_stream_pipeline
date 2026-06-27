# OpenSearch — Reference & How-To

A practical reference: what OpenSearch is, what you can do with it, the query **syntax**
(the Query DSL), and exactly how our code connects to and uses it. The basics of how our
pipeline writes to it are also in [consumer-explained.md](consumer-explained.md).

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
