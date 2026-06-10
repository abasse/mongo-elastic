# mongo-elastic

A production-grade Go service that replicates documents from one or more MongoDB collections to Elasticsearch in real-time using [MongoDB Change Streams](https://www.mongodb.com/docs/manual/changeStreams/).

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
MongoDB (replica set)                          Elasticsearch
┌──────────────────────────┐                  ┌─────────────────────┐
│  orders        ──────────┼─── Change Stream ─►  orders_v2  index  │
│  products      ──────────┼─── Change Stream ─►  products   index  │
│  users         ──────────┼─── Change Stream ─►  user_profiles     │
└──────────────────────────┘                  └─────────────────────┘
     one goroutine per collection
```

1. The service opens a [change stream](https://www.mongodb.com/docs/manual/changeStreams/) per configured collection, running each in its own goroutine. A `$match` pipeline filters only the four write operations.
2. For `insert`, `update`, and `replace` events the document is upserted into Elasticsearch using the MongoDB `_id` (converted to a string) as the Elasticsearch `_id`, guaranteeing a one-to-one mapping and idempotent writes.
3. For `delete` events the corresponding Elasticsearch document is removed.
4. After every event (configurable) each collection's change-stream **resume token** is persisted independently so the service can continue from the exact same position per collection after a restart.
5. If any collection worker exits with a non-recoverable error the whole service stops, ensuring partial replication never goes undetected.

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
# 1. Copy the example env file and adjust values as needed
cp .env.example .env

# 2. Start everything
docker compose up -d

# 3. Tail the replicator logs (one log line per collection worker)
docker compose logs -f replicator
```

To also start Kibana (for exploring the replicated data):

```bash
docker compose --profile kibana up -d
```

### Try it out

Open a `mongosh` session and write to multiple collections:

```js
use mydb

// These writes are replicated concurrently to their respective ES indices.
db.orders.insertOne({ item: "widget", qty: 5, status: "pending" })
db.products.insertOne({ name: "widget", price: 9.99, stock: 100 })

db.orders.updateOne({ item: "widget" }, { $set: { status: "shipped" } })
db.orders.deleteOne({ item: "widget" })
```

Query Elasticsearch to confirm the changes arrived:

```bash
# Check orders index
curl -s http://localhost:9200/orders/_search | jq '.hits.hits'

# Check products index
curl -s http://localhost:9200/products/_search | jq '.hits.hits'
```

---

## Running without Docker

```bash
# Build
go build -o mongo-elastic .

# Watch a single collection (minimal config)
export MONGO_URI="mongodb://localhost:27017/?replicaSet=rs0"
export MONGO_DB="mydb"
export COLLECTIONS="mycollection"
export ES_ADDRESSES="http://localhost:9200"
./mongo-elastic

# Watch multiple collections with custom index names
export COLLECTIONS="orders:orders_v2,products,users:user_profiles"
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

### Collections

| Variable | Default | Description |
|---|---|---|
| `COLLECTIONS` | `mycollection` | Comma-separated list of `collection[:index]` pairs. The Elasticsearch index name is optional; it defaults to the collection name when omitted. See examples below. |

**`COLLECTIONS` examples:**

```bash
# Single collection, index name = collection name
COLLECTIONS=orders

# Single collection with a custom index name
COLLECTIONS=orders:orders_v2

# Multiple collections — mix of default and custom index names
COLLECTIONS=orders:orders_v2,products,users:user_profiles
```

**Legacy fallback:** If `COLLECTIONS` is not set, the service falls back to the `MONGO_COLLECTION` and `ES_INDEX` environment variables (single-collection mode, fully backward-compatible with earlier versions).

### Elasticsearch

| Variable | Default | Description |
|---|---|---|
| `ES_ADDRESSES` | `http://localhost:9200` | Comma-separated list of node addresses. |
| `ES_USERNAME` | *(empty)* | Basic-auth username (optional). |
| `ES_PASSWORD` | *(empty)* | Basic-auth password (optional). |

### Resume tokens

Each collection stores its resume token independently, so a restart mid-way through one collection never affects another.

| Variable | Default | Description |
|---|---|---|
| `TOKEN_STORE` | `file` | `file` — write a JSON file per collection (e.g. `resume_token_orders.json`). `mongo` — upsert a document per collection into a metadata collection; recommended for stateless containers. `s3` — store tokens as JSON objects in an S3 bucket; ideal for serverless or ephemeral deployments. |
| `TOKEN_FILE` | `resume_token.json` | Base path for token files when `TOKEN_STORE=file`. The collection name is inserted before the extension: `resume_token.json` + `orders` → `resume_token_orders.json`. |
| `TOKEN_MONGO_META_COLLECTION` | `_replicator_meta` | Metadata collection used when `TOKEN_STORE=mongo`. Each collection's token is a separate document (`token_orders`, `token_products`, …). |
| `TOKEN_S3_BUCKET` | *(required for s3)* | S3 bucket name when `TOKEN_STORE=s3`. |
| `TOKEN_S3_PREFIX` | `mongo-elastic/` | S3 key prefix when `TOKEN_STORE=s3`. Each collection's token is stored as `<prefix><collection>.json`, e.g. `mongo-elastic/orders.json`. |
| `TOKEN_S3_REGION` | *(SDK default)* | AWS region override for `TOKEN_STORE=s3`. When empty the SDK resolves the region via `AWS_DEFAULT_REGION`, `~/.aws/config`, or EC2/ECS instance metadata. |
| `TOKEN_SAVE_INTERVAL` | `1` | Save the token every N events. `1` gives maximum durability; higher values reduce I/O at the cost of replaying more events after a crash. |

#### S3 token store — AWS credentials

Credentials are resolved automatically by the AWS SDK default credential chain:

1. `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN` environment variables
2. `~/.aws/credentials` and `~/.aws/config` files
3. ECS task role or EC2/EKS instance profile (IAM role)

The IAM principal needs only `s3:GetObject` and `s3:PutObject` on the token objects:

```json
{
  "Effect": "Allow",
  "Action": ["s3:GetObject", "s3:PutObject"],
  "Resource": "arn:aws:s3:::my-token-bucket/mongo-elastic/*"
}
```

#### S3 token store — example

```bash
TOKEN_STORE=s3
TOKEN_S3_BUCKET=my-token-bucket
TOKEN_S3_PREFIX=mongo-elastic/prod/   # optional, default: mongo-elastic/
TOKEN_S3_REGION=us-east-1             # optional

# Standard AWS credential env vars (or use IAM role / ~/.aws/credentials)
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

With `COLLECTIONS=orders,products` the replicator stores two objects:

```
s3://my-token-bucket/mongo-elastic/prod/orders.json
s3://my-token-bucket/mongo-elastic/prod/products.json
```

Each object is a small JSON document:

```json
{"saved_at":"2024-06-01T12:00:00Z","token":"<base64-encoded BSON>"}
```

### Resiliency

| Variable | Default | Description |
|---|---|---|
| `STARTUP_FULL_SYNC` | `false` | Perform a complete scan of every configured collection and bulk-upsert into ES on the first startup, before opening any change stream. Runs concurrently across all collections. Recommended when you cannot guarantee downtime is always shorter than the oplog retention window. |
| `STALE_TOKEN_RESYNC` | `true` | Automatically trigger a full sync for a collection when its resume token is no longer in the oplog. Set to `false` to stop the affected worker with an error instead — useful when manual confirmation is required before resyncing a large collection. |
| `SYNC_BATCH_SIZE` | `500` | Documents per Elasticsearch bulk request during a full sync. Increase for faster syncs; decrease if you hit circuit-breaker or heap-pressure errors. |
| `MAX_RETRIES` | `10` | Maximum retry attempts for connection and per-event Elasticsearch operations. |
| `INITIAL_BACKOFF` | `500ms` | Starting delay for exponential backoff. |
| `MAX_BACKOFF` | `30s` | Upper bound on backoff delay. |

### Logging

| Variable | Default | Description |
|---|---|---|
| `LOG_LEVEL` | `info` | One of `debug`, `info`, `warn`, `error`. Output is structured JSON on stdout. Every log line includes `collection` and `index` fields so you can filter per-collection in any log aggregator. |

---

## Resiliency in depth

### Resume tokens (always on)

After every processed event each collection worker saves its own change stream resume token. On the next startup each worker reads its own token and calls `SetResumeAfter`, resuming from exactly where it left off — no events are missed or duplicated, provided the token is still within the MongoDB oplog. Tokens for different collections never interfere with each other.

```
Startup
  └─ orders worker:   load resume_token_orders.json   → SetResumeAfter → stream continues
  └─ products worker: load resume_token_products.json → SetResumeAfter → stream continues
```

### Stale-token detection and full resync (`STALE_TOKEN_RESYNC=true`)

The MongoDB oplog is a capped collection with a finite retention window. If the service is down *longer than that window*, the saved token is gone and the stream cannot resume. MongoDB returns error code **136** (`CappedPositionLost`) or **286** (`ChangeStreamHistoryLost`) in this case.

When `STALE_TOKEN_RESYNC=true` (the default), the affected worker automatically runs a **full sync**:

```
Stale token error on "orders" worker
  └─ Capture cluster timestamp T0
  └─ Cursor-scan entire "orders" collection
  └─ Bulk-upsert all documents into "orders_v2" ES index
  └─ SetStartAtOperationTime(T0)  ← stream opens at T0, replaying events
                                     that arrived during the scan (idempotent)
```

Other collection workers are **not affected** — they continue streaming normally while the stale worker resyncs.

Setting `STALE_TOKEN_RESYNC=false` causes the affected worker to exit with a descriptive error, which propagates to the orchestrator and stops the whole service.

### Startup full sync (`STARTUP_FULL_SYNC=true`)

Forces the full-sync procedure for **all collections** on the first iteration of every process startup, before any change stream is opened. All collection syncs run concurrently. This is the safest option when:

- The oplog window is short relative to potential maintenance downtime.
- You are deploying for the first time and want ES to be immediately consistent with MongoDB.
- You cannot predict how long the service may be down.

The sync is idempotent: existing ES documents with the same `_id` are overwritten in place.

### Exponential backoff

Backoff with ±10 % jitter is applied in three places per worker:

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
│   │   └── config.go               # Env-var config loading; COLLECTIONS parsing;
│   │                               # CollectionMapping type
│   └── replicator/
│       ├── replicator.go           # Orchestrator: New, Run, Close; shared clients;
│       │                           # BSON→JSON helpers; stale-token error detection
│       ├── worker.go               # collectionWorker: per-collection change-stream
│       │                           # loop, event processing, ES write operations
│       ├── fullsync.go             # FullSync: full collection scan + ES bulk indexer;
│       │                           # cluster-time / sync-marker token helpers
│       ├── token.go                # TokenStore interface; FileTokenStore (one file per
│       │                           # collection); MongoTokenStore (one doc per collection);
│       │                           # S3TokenStore (one S3 object per collection)
│       └── backoff.go              # Exponential backoff with jitter
│
├── scripts/
│   └── mongo-init.js               # rs.initiate() for the Docker Compose replica set
│
├── Dockerfile                      # Multi-stage build → minimal scratch image
├── docker-compose.yml              # MongoDB + Elasticsearch + replicator (+ optional Kibana)
└── .env.example                    # All environment variables with documentation
```
