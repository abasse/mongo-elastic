// Package replicator streams MongoDB change events into Elasticsearch in
// real-time, with resume-token persistence and exponential-backoff retries.
// Multiple collections are replicated concurrently, each in its own goroutine.
package replicator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/sync/errgroup"

	"github.com/abasse/mongo-elastic/internal/config"
)

// ─────────────────────────────────────────────────────────────────────────────
// Change-event types  (shared across all workers)
// ─────────────────────────────────────────────────────────────────────────────

// changeEvent mirrors the shape of a MongoDB change stream document.
type changeEvent struct {
	ID            bson.Raw `bson:"_id"`
	OperationType string   `bson:"operationType"`
	FullDocument  bson.Raw `bson:"fullDocument"`
	DocumentKey   struct {
		ID bson.RawValue `bson:"_id"`
	} `bson:"documentKey"`
	NS struct {
		DB   string `bson:"db"`
		Coll string `bson:"coll"`
	} `bson:"ns"`
	UpdateDescription *updateDescription `bson:"updateDescription"`
}

type updateDescription struct {
	UpdatedFields bson.Raw `bson:"updatedFields"`
	RemovedFields []string `bson:"removedFields"`
}

// ─────────────────────────────────────────────────────────────────────────────
// Replicator  (orchestrator)
// ─────────────────────────────────────────────────────────────────────────────

// Replicator manages one collectionWorker per configured collection and runs
// them concurrently.  All workers share the same MongoDB and Elasticsearch
// client connections.
type Replicator struct {
	logger      *slog.Logger
	mongoClient *mongo.Client
	workers     []*collectionWorker
}

// New dials MongoDB and Elasticsearch (with retry) and creates one worker per
// collection mapping.  Call Close() when done to release connections.
func New(cfg *config.Config, logger *slog.Logger) (*Replicator, error) {
	retryCfg := RetryConfig{
		MaxRetries:     cfg.MaxRetries,
		InitialBackoff: cfg.InitialBackoff,
		MaxBackoff:     cfg.MaxBackoff,
	}

	// ── Connect to MongoDB ────────────────────────────────────────────────────
	var mongoClient *mongo.Client
	err := withRetry(context.Background(), retryCfg, logger, "connect MongoDB", func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		c, err := mongo.Connect(ctx, options.Client().ApplyURI(cfg.MongoURI))
		if err != nil {
			return err
		}
		if err := c.Ping(ctx, nil); err != nil {
			_ = c.Disconnect(context.Background())
			return err
		}
		mongoClient = c
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("MongoDB connection failed: %w", err)
	}
	logger.Info("connected to MongoDB", "uri", cfg.MongoURI, "db", cfg.MongoDB)

	mongoDB := mongoClient.Database(cfg.MongoDB)

	// ── Connect to Elasticsearch ──────────────────────────────────────────────
	var esClient *elasticsearch.Client
	err = withRetry(context.Background(), retryCfg, logger, "connect Elasticsearch", func() error {
		c, err := elasticsearch.NewClient(elasticsearch.Config{
			Addresses: cfg.ESAddresses,
			Username:  cfg.ESUsername,
			Password:  cfg.ESPassword,
		})
		if err != nil {
			return err
		}
		res, err := c.Info(c.Info.WithContext(context.Background()))
		if err != nil {
			return err
		}
		defer res.Body.Close()
		if res.IsError() {
			return fmt.Errorf("ES info: %s", res.Status())
		}
		esClient = c
		return nil
	})
	if err != nil {
		_ = mongoClient.Disconnect(context.Background())
		return nil, fmt.Errorf("Elasticsearch connection failed: %w", err)
	}
	logger.Info("connected to Elasticsearch", "addresses", cfg.ESAddresses)

	// ── Build one worker per collection mapping ───────────────────────────────
	var metaColl *mongo.Collection
	if cfg.TokenStore == config.TokenStoreMongo {
		metaColl = mongoDB.Collection(cfg.TokenMongoMeta)
	}

	workers := make([]*collectionWorker, 0, len(cfg.Collections))
	for _, mapping := range cfg.Collections {
		wlog := logger.With(
			"collection", mapping.Collection,
			"index", mapping.Index,
		)

		var tokenStore TokenStore
		if cfg.TokenStore == config.TokenStoreMongo {
			tokenStore = NewMongoTokenStore(metaColl, mapping.Collection, wlog)
		} else {
			tokenStore = NewFileTokenStore(cfg.TokenFilePath, mapping.Collection, wlog)
		}

		workers = append(workers, &collectionWorker{
			mapping:     mapping,
			mongoColl:   mongoDB.Collection(mapping.Collection),
			mongoClient: mongoClient,
			esClient:    esClient,
			tokenStore:  tokenStore,
			retryCfg:    retryCfg,
			cfg:         cfg,
			logger:      wlog,
		})

		wlog.Info("collection worker created")
	}

	return &Replicator{
		logger:      logger,
		mongoClient: mongoClient,
		workers:     workers,
	}, nil
}

// Close disconnects from MongoDB.
func (r *Replicator) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.mongoClient.Disconnect(ctx); err != nil {
		r.logger.Warn("error disconnecting from MongoDB", "error", err)
	}
}

// Run starts one goroutine per collection worker and blocks until all workers
// have stopped.  If any worker exits with a non-recoverable error the context
// is cancelled, which causes all other workers to stop gracefully.
func (r *Replicator) Run(ctx context.Context) error {
	g, gctx := errgroup.WithContext(ctx)
	for _, w := range r.workers {
		w := w // capture loop variable
		g.Go(func() error {
			return w.run(gctx)
		})
	}
	return g.Wait()
}

// ─────────────────────────────────────────────────────────────────────────────
// Error classification
// ─────────────────────────────────────────────────────────────────────────────

// isStaleResumeTokenError reports whether err indicates that the saved resume
// token is no longer present in the MongoDB oplog.
//
// Known error codes:
//   - 136  CappedPositionLost      – oplog rolled past the resume token
//   - 280  ChangeStreamFatalError  – generic fatal change-stream error
//   - 286  ChangeStreamHistoryLost – explicit "history lost" (MongoDB 6+)
func isStaleResumeTokenError(err error) bool {
	if err == nil {
		return false
	}
	var cmdErr mongo.CommandError
	if errors.As(err, &cmdErr) {
		switch cmdErr.Code {
		case 136, 280, 286:
			return true
		}
	}
	msg := err.Error()
	return strings.Contains(msg, "resume point may no longer be in the oplog") ||
		strings.Contains(msg, "ChangeStreamHistoryLost") ||
		strings.Contains(msg, "CappedPositionLost")
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared BSON / JSON conversion helpers
// ─────────────────────────────────────────────────────────────────────────────

// bsonToJSON converts a BSON raw document to standard JSON.
// ObjectIDs become 24-char hex strings; other BSON-only types are normalised
// so that no MongoDB extended-JSON syntax appears in Elasticsearch payloads.
func bsonToJSON(raw bson.Raw) ([]byte, error) {
	if raw == nil {
		return []byte("{}"), nil
	}
	var v interface{}
	if err := bson.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	return json.Marshal(normalizeBSON(v))
}

// normalizeBSON recursively converts BSON primitives to JSON-serialisable Go
// types understood by encoding/json.
func normalizeBSON(v interface{}) interface{} {
	switch t := v.(type) {
	case primitive.ObjectID:
		return t.Hex()
	case primitive.DateTime:
		return t.Time().UTC().Format(time.RFC3339Nano)
	case primitive.Timestamp:
		return map[string]interface{}{"t": t.T, "i": t.I}
	case primitive.Binary:
		return map[string]interface{}{"subType": fmt.Sprintf("%02x", t.Subtype), "data": t.Data}
	case primitive.Decimal128:
		return t.String()
	case primitive.Regex:
		return map[string]interface{}{"pattern": t.Pattern, "options": t.Options}
	case primitive.JavaScript:
		return string(t)
	case primitive.DBPointer:
		return map[string]interface{}{"db": t.DB, "id": t.Pointer.Hex()}
	case bson.M:
		out := make(map[string]interface{}, len(t))
		for k, val := range t {
			out[k] = normalizeBSON(val)
		}
		return out
	case bson.D:
		out := make(map[string]interface{}, len(t))
		for _, elem := range t {
			out[elem.Key] = normalizeBSON(elem.Value)
		}
		return out
	case bson.A:
		out := make([]interface{}, len(t))
		for i, elem := range t {
			out[i] = normalizeBSON(elem)
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(t))
		for i, elem := range t {
			out[i] = normalizeBSON(elem)
		}
		return out
	default:
		return v
	}
}

// rawValueToString converts a bson.RawValue _id into a stable string suitable
// for use as an Elasticsearch document ID.
func rawValueToString(v bson.RawValue) (string, error) {
	switch v.Type {
	case bsontype.ObjectID:
		return v.ObjectID().Hex(), nil
	case bsontype.String:
		s, _ := v.StringValueOK()
		return s, nil
	case bsontype.Int32:
		return fmt.Sprintf("%d", v.Int32()), nil
	case bsontype.Int64:
		return fmt.Sprintf("%d", v.Int64()), nil
	case bsontype.Double:
		return fmt.Sprintf("%g", v.Double()), nil
	case bsontype.Boolean:
		if v.Boolean() {
			return "true", nil
		}
		return "false", nil
	case bsontype.Undefined, bsontype.Null:
		return "", fmt.Errorf("document _id is null or undefined")
	default:
		extJSON, err := bson.MarshalExtJSON(bson.Raw(v.Value), true, false)
		if err != nil {
			return "", fmt.Errorf("unsupported _id type %s: %w", v.Type, err)
		}
		return string(extJSON), nil
	}
}

