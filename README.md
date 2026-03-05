# mongo-elastic

A production-grade Go service that replicates documents from a MongoDB collection to an Elasticsearch index in real-time using [MongoDB Change Streams](https://www.mongodb.com/docs/manual/changeStreams/).

## Contents

- [How it works](#how-it-works)
- [Prerequisites](#prerequisites)
- [Quick start (Docker Compose)](#quick-start-docker-compose)
- [Running without Docker](#running-without-docker)
- [Configuration reference](#configuration-reference)
- [Resiliency in depth](#resiliency-in-depth)
- [Project layout](#project-layout)

---

## How it works

```
MongoDB (replica set)                      Elasticsearch
┌─────────────────────┐                   ┌──────────────┐
│  collection         │  Change Stream    │              │
│  ┌───────────────┐  │ ───────────────►  │    index     │
│  │ insert        │  │   upsert (index)  │              │
│  │ update        │  │   upsert (index)  │              │
│  │ replace       │  │   upsert (index)  └──────────────┘
│  │ delete        │  │   delete
│  └───────────────┘  │
└─────────────────────┘
```

1. The service opens a [change stream](https://www.mongodb.com/docs/manual/changeStreams/) on a configured MongoDB collection using `$match` to filter only the four write operations.
2. For `insert`, `update`, and `replace` events the document is upserted into Elasticsearch using the MongoDB `_id` (converted to a string) as the Elasticsearch `_id`, guaranteeing a one-to-one mapping and idempotent writes.
3. For `delete` events the corresponding Elasticsearch document is removed.
4. After every event (configurable) the change-stream **resume token** is persisted so the service can continue from the exact same position after a restart.

### BSON → JSON conversion

MongoDB BSON types that have no direct JSON equivalent are normalised before being sent to Elasticsearch:

| BSON type | Elasticsearch representation |
|---|---|
| `ObjectID` | 24-character hex string |
| `DateTime` | RFC 3339 string (`2024-01-15T12:00:00Z`) |
| `Decimal128` | string |
| `Timestamp` | `{"t": <secs>, "i": <ordinal>}` |
| `Binary` | `{"subType": "00", "data": <base64>}` |
| `Regex` | `{"pattern": "...", "options": "..."}` |

---

## Prerequisites

| Requirement | Notes |
|---|---|
| Go 1.21+ | Uses `log/slog` from the standard library |
| MongoDB 6.0+ | Must be running as a **replica set** (change streams require a replica set or sharded cluster) |
| Elasticsearch 8.x | Tested against 8.13 |

> **Why a replica set?** MongoDB change streams are built on the oplog, which is only written by replica set members. A standalone `mongod` cannot produce a change stream.

---

## Quick start (Docker Compose)

The included `docker-compose.yml` starts MongoDB (single-node replica set), Elasticsearch, and the replicator itself.

```bash
# 1. Copy the example env file and adjust values if needed
cp .env.example .env

# 2. Start everything
docker compose up -d

# 3. Tail the replicator logs
docker compose logs -f replicator
```

To also start Kibana (for exploring the replicated data):

```bash
docker compose --profile kibana up -d
```

### Try it out

Open a `mongosh` session and insert some documents:

```js
use mydb
db.mycollection.insertOne({ name: "Alice", age: 30 })
db.mycollection.updateOne({ name: "Alice" }, { $set: { age: 31 } })
db.mycollection.deleteOne({ name: "Alice" })
```

Query Elasticsearch to confirm the changes arrived:

```bash
# After insert / update
curl -s http://localhost:9200/mycollection/_search | jq '.hits.hits'

# After delete — document should be gone
curl -s http://localhost:9200/mycollection/_doc/<id>
```

---

## Running without Docker

```bash
# Build
go build -o mongo-elastic .

# Set required variables
export MONGO_URI="mongodb://localhost:27017/?replicaSet=rs0"
export MONGO_DB="mydb"
export MONGO_COLLECTION="mycollection"
export ES_ADDRESSES="http://localhost:9200"

# Run
./mongo-elastic
```

---

## Configuration reference

All settings are read from environment variables. Copy `.env.example` to `.env` as a starting point.

### MongoDB

| Variable | Default | Description |
|---|---|---|
| `MONGO_URI` | `mongodb://localhost:27017` | Connection URI. Must include `?replicaSet=<name>` for change streams. |
| `MONGO_DB` | `mydb` | Source database. |
| `MONGO_COLLECTION` | `mycollection` | Source collection to watch. |

### Elasticsearch

| Variable | Default | Description |
|---|---|---|
| `ES_ADDRESSES` | `http://localhost:9200` | Comma-separated list of node addresses. |
| `ES_USERNAME` | *(empty)* | Basic-auth username (optional). |
| `ES_PASSWORD` | *(empty)* | Basic-auth password (optional). |
| `ES_INDEX` | *(same as `MONGO_COLLECTION`)* | Target index name. |

### Resume token

| Variable | Default | Description |
|---|---|---|
| `TOKEN_STORE` | `file` | Where to persist the resume token. `file` writes a local JSON file; `mongo` upserts into a MongoDB collection — better for stateless containers. |
| `TOKEN_FILE` | `resume_token.json` | Path to the token file when `TOKEN_STORE=file`. |
| `TOKEN_MONGO_META_COLLECTION` | `_replicator_meta` | Metadata collection name when `TOKEN_STORE=mongo`. |
| `TOKEN_SAVE_INTERVAL` | `1` | Save the token every N events. `1` gives maximum durability; higher values reduce disk/network I/O at the cost of replaying more events after a crash. |

### Resiliency

| Variable | Default | Description |
|---|---|---|
| `STARTUP_FULL_SYNC` | `false` | Perform a complete collection scan and bulk-upsert into ES on every first startup before opening the change stream. Recommended when you cannot guarantee downtime is always shorter than the oplog retention window. |
| `STALE_TOKEN_RESYNC` | `true` | Automatically trigger a full sync when the saved resume token is no longer in the oplog (service was down longer than oplog retention). Set to `false` to make the service exit with an error instead — useful when manual confirmation is required before resyncing a large collection. |
| `SYNC_BATCH_SIZE` | `500` | Documents per Elasticsearch bulk request during a full sync. Increase for faster syncs; decrease if you hit circuit-breaker or heap-pressure errors. |
| `MAX_RETRIES` | `10` | Maximum retry attempts for connection and per-event Elasticsearch operations. |
| `INITIAL_BACKOFF` | `500ms` | Starting delay for exponential backoff. |
| `MAX_BACKOFF` | `30s` | Upper bound on backoff delay. |

### Logging

| Variable | Default | Description |
|---|---|---|
| `LOG_LEVEL` | `info` | One of `debug`, `info`, `warn`, `error`. Output is structured JSON on stdout. |

---

## Resiliency in depth

### Resume tokens (always on)

After every processed event the service saves the change stream's resume token. On the next startup it reads the token and passes it to `SetResumeAfter`, so the stream resumes from exactly where it left off — no events are missed or duplicated, provided the token is still within the MongoDB oplog.

```
Startup
  └─ Load token → SetResumeAfter(token) → change stream continues
```

### Stale-token detection and full resync (`STALE_TOKEN_RESYNC=true`)

The MongoDB oplog is a capped collection with a finite retention window. If the service is down *longer than that window*, the saved token is gone and the stream cannot resume. MongoDB returns error code **136** (`CappedPositionLost`) or **286** (`ChangeStreamHistoryLost`) in this case.

When `STALE_TOKEN_RESYNC=true` (the default), the service detects these errors and automatically runs a **full sync**:

```
Stale token error detected
  └─ Capture cluster timestamp T0
  └─ Cursor-scan entire collection
  └─ Bulk-upsert all documents into ES
  └─ SetStartAtOperationTime(T0)  ← stream opens at T0, replaying events
                                     that arrived during the scan (idempotent)
```

Setting `STALE_TOKEN_RESYNC=false` makes the service exit with a descriptive error message instead, which lets an operator decide whether to trigger the resync manually (e.g. via `STARTUP_FULL_SYNC=true`).

### Startup full sync (`STARTUP_FULL_SYNC=true`)

Forces the full-sync procedure described above on the first iteration of every process startup, regardless of whether the resume token is stale. This is the safest option when:

- The oplog window is short relative to potential maintenance downtime.
- You are deploying for the first time and want ES to be immediately consistent with the current state of MongoDB.
- You cannot control or predict how long the service may be down.

The sync is idempotent: existing ES documents with the same `_id` are overwritten in place.

### Exponential backoff

Backoff with ±10 % jitter is applied in three places:

| Situation | Effect |
|---|---|
| Initial MongoDB / ES dial fails | Retry up to `MAX_RETRIES` times before exiting |
| ES operation fails for a single event | Retry in-place; the stream stays open |
| Change stream cycle ends with a non-stale error | Wait before reopening the stream |

---

## Project layout

```
mongo-elastic/
├── main.go                         # Entry point: logging, config, signal handling
│
├── internal/
│   ├── config/
│   │   └── config.go               # Env-var config loading and validation
│   │
│   └── replicator/
│       ├── replicator.go           # Core: change-stream loop, ES write operations,
│       │                           # stale-token detection, BSON→JSON conversion
│       ├── fullsync.go             # Full collection scan + Elasticsearch bulk indexer
│       ├── token.go                # Resume-token persistence (file or MongoDB)
│       └── backoff.go              # Exponential backoff with jitter
│
├── scripts/
│   └── mongo-init.js               # rs.initiate() for the Docker Compose replica set
│
├── Dockerfile                      # Multi-stage build → minimal scratch image
├── docker-compose.yml              # MongoDB + Elasticsearch + replicator (+ optional Kibana)
└── .env.example                    # All environment variables with documentation
```
