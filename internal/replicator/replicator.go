// Package replicator streams MongoDB change events into Elasticsearch in
// real-time, with resume-token persistence and exponential-backoff retries.
package replicator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/abasse/mongo-elastic/internal/config"
)

// ─────────────────────────────────────────────────────────────────────────────
// Change-event types
// ─────────────────────────────────────────────────────────────────────────────

// changeEvent mirrors the shape of a MongoDB change stream document.
type changeEvent struct {
	// Resume token (the change-stream cursor position).
	ID bson.Raw `bson:"_id"`

	// "insert", "update", "replace", "delete", "invalidate", …
	OperationType string `bson:"operationType"`

	// Populated for insert / replace, and for update when UpdateLookup is used.
	FullDocument bson.Raw `bson:"fullDocument"`

	// Always present for document operations.
	DocumentKey struct {
		ID bson.RawValue `bson:"_id"`
	} `bson:"documentKey"`

	// Namespace of the affected document.
	NS struct {
		DB   string `bson:"db"`
		Coll string `bson:"coll"`
	} `bson:"ns"`

	// Present for "update" events: fields that changed and fields removed.
	UpdateDescription *updateDescription `bson:"updateDescription"`
}

type updateDescription struct {
	UpdatedFields bson.Raw `bson:"updatedFields"`
	RemovedFields []string `bson:"removedFields"`
}

// ─────────────────────────────────────────────────────────────────────────────
// Replicator
// ─────────────────────────────────────────────────────────────────────────────

// Replicator watches a MongoDB collection and replicates every document change
// into an Elasticsearch index.
type Replicator struct {
	cfg         *config.Config
	logger      *slog.Logger
	retryCfg    RetryConfig
	mongoClient *mongo.Client
	mongoColl   *mongo.Collection
	esClient    *elasticsearch.Client
	tokenStore  TokenStore

	// postSyncClusterTime is set by FullSync so that the next runOnce call
	// opens the change stream with SetStartAtOperationTime instead of
	// SetResumeAfter.  This guarantees no gap between the full scan and the
	// first live event.  runOnce clears the field after consuming it.
	postSyncClusterTime *primitive.Timestamp
}

// New dials both MongoDB and Elasticsearch (with retry) and returns a ready
// Replicator. Call Close() when done to release connections.
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

		clientOpts := options.Client().ApplyURI(cfg.MongoURI)
		c, err := mongo.Connect(ctx, clientOpts)
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
	logger.Info("connected to MongoDB", "uri", cfg.MongoURI, "db", cfg.MongoDB, "collection", cfg.MongoCollection)

	mongoDB := mongoClient.Database(cfg.MongoDB)
	mongoColl := mongoDB.Collection(cfg.MongoCollection)

	// ── Build token store ─────────────────────────────────────────────────────
	var tokenStore TokenStore
	switch cfg.TokenStore {
	case config.TokenStoreMongo:
		metaColl := mongoDB.Collection(cfg.TokenMongoMeta)
		tokenStore = NewMongoTokenStore(metaColl, logger)
	default:
		tokenStore = NewFileTokenStore(cfg.TokenFilePath, logger)
	}

	// ── Connect to Elasticsearch ──────────────────────────────────────────────
	var esClient *elasticsearch.Client
	err = withRetry(context.Background(), retryCfg, logger, "connect Elasticsearch", func() error {
		esCfg := elasticsearch.Config{
			Addresses: cfg.ESAddresses,
			Username:  cfg.ESUsername,
			Password:  cfg.ESPassword,
		}
		c, err := elasticsearch.NewClient(esCfg)
		if err != nil {
			return err
		}
		// Validate connectivity by calling the Info API.
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
	logger.Info("connected to Elasticsearch", "addresses", cfg.ESAddresses, "index", cfg.ESIndex)

	return &Replicator{
		cfg:         cfg,
		logger:      logger,
		retryCfg:    retryCfg,
		mongoClient:  mongoClient,
		mongoColl:   mongoColl,
		esClient:    esClient,
		tokenStore:  tokenStore,
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

// Run is the service's main loop. It opens the change stream, processes
// events, and automatically reconnects (with backoff) whenever the stream or
// the Elasticsearch connection is lost.  It returns only when ctx is cancelled.
//
// If STARTUP_FULL_SYNC=true the service performs a full collection scan before
// opening the change stream on the very first iteration.  Subsequent
// reconnection cycles (after errors) do NOT repeat the full sync unless a
// stale-token error triggers one automatically.
func (r *Replicator) Run(ctx context.Context) error {
	firstRun := true

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		// On the very first run, honour STARTUP_FULL_SYNC.
		if firstRun && r.cfg.StartupFullSync {
			r.logger.Info("startup full sync requested")
			if err := r.FullSync(ctx); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return ctx.Err()
				}
				r.logger.Error("startup full sync failed, will retry after backoff", "error", err)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(backoffDelay(r.cfg.InitialBackoff, r.cfg.MaxBackoff, 0)):
				}
				continue // retry the full sync
			}
		}
		firstRun = false

		err := r.runOnce(ctx)
		if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return ctx.Err()
		}

		// If the oplog has rolled past our resume token, optionally do a full
		// resync before reopening the stream (controlled by STALE_TOKEN_RESYNC).
		if isStaleResumeTokenError(err) {
			if !r.cfg.StaleTokenResync {
				r.logger.Error("resume token is stale and STALE_TOKEN_RESYNC=false — exiting",
					"error", err,
					"hint", "set STALE_TOKEN_RESYNC=true or STARTUP_FULL_SYNC=true to recover automatically",
				)
				return fmt.Errorf("stale resume token (automatic resync disabled): %w", err)
			}
			r.logger.Warn("resume token is no longer in the oplog — triggering full sync to recover",
				"error", err)
			if syncErr := r.FullSync(ctx); syncErr != nil {
				if errors.Is(syncErr, context.Canceled) || errors.Is(syncErr, context.DeadlineExceeded) {
					return ctx.Err()
				}
				r.logger.Error("full sync after stale token failed", "error", syncErr)
			}
			// Fall through to the reconnect delay and reopen the stream.
		} else {
			r.logger.Error("replication cycle ended with error, restarting", "error", err)
		}

		// Brief pause before reconnecting to avoid a tight spin on persistent errors.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoffDelay(r.cfg.InitialBackoff, r.cfg.MaxBackoff, 0)):
		}
	}
}

// runOnce opens a single change-stream session and processes events until the
// context is cancelled or an unrecoverable error occurs.
func (r *Replicator) runOnce(ctx context.Context) error {
	// ── Determine stream start position ──────────────────────────────────────
	// Priority order:
	//   1. postSyncClusterTime set by FullSync  → SetStartAtOperationTime
	//   2. Saved resume token (real change-stream token) → SetResumeAfter
	//   3. Nothing                              → start from "now"

	csOpts := options.ChangeStream().
		SetFullDocument(options.UpdateLookup).
		SetMaxAwaitTime(10 * time.Second)

	// Consume the post-sync cluster time if FullSync just ran.
	if r.postSyncClusterTime != nil {
		ts := r.postSyncClusterTime
		r.postSyncClusterTime = nil // consume once
		csOpts.SetStartAtOperationTime(ts)
		r.logger.Info("opening change stream at post-sync cluster time",
			"t", ts.T, "i", ts.I)
	} else {
		resumeToken, err := r.tokenStore.Load()
		if err != nil {
			r.logger.Warn("could not load resume token, starting from now", "error", err)
		}
		switch {
		case resumeToken != nil && isSyncMarkerToken(resumeToken):
			// The stored token is a sync-marker (FullSync ran previously but
			// the process was restarted before runOnce consumed postSyncClusterTime).
			if ts, ok := clusterTimeFromSyncToken(resumeToken); ok {
				csOpts.SetStartAtOperationTime(ts)
				r.logger.Info("opening change stream at persisted cluster time (from sync marker)",
					"t", ts.T, "i", ts.I)
			} else {
				r.logger.Warn("sync marker token has no cluster time, starting from now")
			}
		case resumeToken != nil:
			csOpts.SetResumeAfter(resumeToken)
			r.logger.Info("resuming change stream from saved resume token")
		default:
			r.logger.Info("opening change stream from the current cluster time (no token)")
		}
	}

	// Filter only the operation types we care about.
	pipeline := mongo.Pipeline{
		bson.D{{Key: "$match", Value: bson.D{
			{Key: "operationType", Value: bson.D{
				{Key: "$in", Value: bson.A{"insert", "update", "replace", "delete"}},
			}},
		}}},
	}

	cs, err := r.mongoColl.Watch(ctx, pipeline, csOpts)
	if err != nil {
		return fmt.Errorf("watch: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if cerr := cs.Close(closeCtx); cerr != nil {
			r.logger.Warn("error closing change stream", "error", cerr)
		}
	}()

	r.logger.Info("change stream opened",
		"db", r.cfg.MongoDB,
		"collection", r.cfg.MongoCollection,
	)

	eventCount := 0

	for cs.Next(ctx) {
		var event changeEvent
		if err := cs.Decode(&event); err != nil {
			return fmt.Errorf("decode event: %w", err)
		}

		log := r.logger.With(
			"operationType", event.OperationType,
			"ns", fmt.Sprintf("%s.%s", event.NS.DB, event.NS.Coll),
		)

		// Process with retry so transient ES errors don't kill the stream.
		err := withRetry(ctx, r.retryCfg, log, "process event", func() error {
			return r.processEvent(ctx, &event, log)
		})
		if err != nil {
			// Cancelled during retry → propagate.
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			// Log and continue; we don't want one bad document to stall the stream.
			log.Error("failed to process event after retries, skipping", "error", err)
		}

		eventCount++

		// Save the resume token according to the configured interval.
		if eventCount%r.cfg.TokenSaveInterval == 0 {
			if serr := r.tokenStore.Save(cs.ResumeToken()); serr != nil {
				log.Warn("failed to save resume token", "error", serr)
			}
		}
	}

	// cs.Next returned false — check why.
	if err := cs.Err(); err != nil {
		return fmt.Errorf("change stream error: %w", err)
	}
	return nil // clean close (context cancelled)
}

// processEvent dispatches a single change event to the appropriate ES action.
func (r *Replicator) processEvent(ctx context.Context, event *changeEvent, log *slog.Logger) error {
	docID, err := rawValueToString(event.DocumentKey.ID)
	if err != nil {
		return fmt.Errorf("extract document id: %w", err)
	}

	log = log.With("mongo_id", docID)

	switch event.OperationType {
	case "insert", "replace":
		if event.FullDocument == nil {
			return fmt.Errorf("fullDocument is nil for %s event (id=%s)", event.OperationType, docID)
		}
		return r.indexDocument(ctx, docID, event.FullDocument, log)

	case "update":
		if event.FullDocument != nil {
			// UpdateLookup succeeded — push the complete document.
			return r.indexDocument(ctx, docID, event.FullDocument, log)
		}
		// UpdateLookup returned nil (document deleted between update and lookup).
		// Fall back to a partial update using the delta from updateDescription.
		if event.UpdateDescription != nil {
			return r.partialUpdate(ctx, docID, event.UpdateDescription, log)
		}
		log.Warn("update event has neither fullDocument nor updateDescription, skipping")
		return nil

	case "delete":
		return r.deleteDocument(ctx, docID, log)

	default:
		log.Warn("unhandled operation type, skipping", "operationType", event.OperationType)
		return nil
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Elasticsearch helpers
// ─────────────────────────────────────────────────────────────────────────────

// indexDocument upserts a full MongoDB document into Elasticsearch.
func (r *Replicator) indexDocument(ctx context.Context, id string, doc bson.Raw, log *slog.Logger) error {
	body, err := bsonToJSON(doc)
	if err != nil {
		return fmt.Errorf("bson→json: %w", err)
	}

	req := esapi.IndexRequest{
		Index:      r.cfg.ESIndex,
		DocumentID: id,
		Body:       bytes.NewReader(body),
		Refresh:    "false", // avoid forcing a merge on every event
	}

	res, err := req.Do(ctx, r.esClient)
	if err != nil {
		return fmt.Errorf("ES index request: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		errBody, _ := io.ReadAll(res.Body)
		return fmt.Errorf("ES index %s: %s — %s", id, res.Status(), errBody)
	}

	log.Info("document indexed", "es_id", id, "index", r.cfg.ESIndex)
	return nil
}

// partialUpdate applies the MongoDB update delta to an existing ES document
// using the update API's doc-as-upsert semantics. This is the fallback for
// update events where the full document could not be retrieved.
func (r *Replicator) partialUpdate(ctx context.Context, id string, ud *updateDescription, log *slog.Logger) error {
	// Build a flat JSON object with only the updated fields.
	updatedJSON, err := bsonToJSON(ud.UpdatedFields)
	if err != nil {
		return fmt.Errorf("updatedFields bson→json: %w", err)
	}

	// Decode into a map so we can add the removed-fields nulls.
	var patch map[string]interface{}
	if err := json.Unmarshal(updatedJSON, &patch); err != nil {
		return fmt.Errorf("patch unmarshal: %w", err)
	}
	for _, field := range ud.RemovedFields {
		patch[field] = nil
	}

	patchJSON, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("patch marshal: %w", err)
	}

	// Wrap in {"doc": ..., "doc_as_upsert": true} for the ES update API.
	body := fmt.Sprintf(`{"doc":%s,"doc_as_upsert":true}`, patchJSON)

	req := esapi.UpdateRequest{
		Index:      r.cfg.ESIndex,
		DocumentID: id,
		Body:       bytes.NewBufferString(body),
		Refresh:    "false",
	}

	res, err := req.Do(ctx, r.esClient)
	if err != nil {
		return fmt.Errorf("ES update request: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		errBody, _ := io.ReadAll(res.Body)
		return fmt.Errorf("ES update %s: %s — %s", id, res.Status(), errBody)
	}

	log.Info("document partially updated", "es_id", id, "index", r.cfg.ESIndex,
		"removed_fields", ud.RemovedFields)
	return nil
}

// deleteDocument removes a document from Elasticsearch by its ID.
// A 404 response is treated as success (already absent ≡ deleted).
func (r *Replicator) deleteDocument(ctx context.Context, id string, log *slog.Logger) error {
	req := esapi.DeleteRequest{
		Index:      r.cfg.ESIndex,
		DocumentID: id,
		Refresh:    "false",
	}

	res, err := req.Do(ctx, r.esClient)
	if err != nil {
		return fmt.Errorf("ES delete request: %w", err)
	}
	defer res.Body.Close()

	// 404 is fine: the document was already absent.
	if res.StatusCode == 404 {
		log.Warn("document not found in ES during delete (already absent)", "es_id", id)
		return nil
	}
	if res.IsError() {
		errBody, _ := io.ReadAll(res.Body)
		return fmt.Errorf("ES delete %s: %s — %s", id, res.Status(), errBody)
	}

	log.Info("document deleted", "es_id", id, "index", r.cfg.ESIndex)
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Error classification
// ─────────────────────────────────────────────────────────────────────────────

// isStaleResumeTokenError reports whether err indicates that the saved resume
// token is no longer present in the MongoDB oplog.  This happens when the
// service was down longer than the oplog retention window.
//
// Known error codes:
//   - 136  CappedPositionLost      – oplog (a capped collection) rolled past the token
//   - 280  ChangeStreamFatalError  – generic fatal change-stream error
//   - 286  ChangeStreamHistoryLost – explicit "history lost" error (MongoDB 6+)
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
	// Fallback: check the error string for older driver / server versions.
	msg := err.Error()
	return strings.Contains(msg, "resume point may no longer be in the oplog") ||
		strings.Contains(msg, "ChangeStreamHistoryLost") ||
		strings.Contains(msg, "CappedPositionLost")
}

// ─────────────────────────────────────────────────────────────────────────────
// Conversion helpers
// ─────────────────────────────────────────────────────────────────────────────

// bsonToJSON converts a BSON raw document to canonical JSON.
// It unmarshals through bson.M so that Go's encoding/json handles the final
// serialisation, which avoids MongoDB extended-JSON syntax in ES payloads.
// ObjectIDs are rendered as their 24-character hex strings.
func bsonToJSON(raw bson.Raw) ([]byte, error) {
	if raw == nil {
		return []byte("{}"), nil
	}
	// Unmarshal BSON → Go value (bson.M / bson.A / primitives).
	var v interface{}
	if err := bson.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	// Convert any BSON-specific types to JSON-friendly equivalents recursively.
	v = normalizeBSON(v)
	return json.Marshal(v)
}

// normalizeBSON recursively converts BSON primitives that don't have a natural
// JSON representation into plain Go types that encoding/json can handle.
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

// rawValueToString converts a bson.RawValue holding a document's _id into a
// stable, non-empty string suitable for use as an Elasticsearch document ID.
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
		// For any other type (embedded doc, binary, …) marshal to extended JSON.
		extJSON, err := bson.MarshalExtJSON(bson.Raw(v.Value), true, false)
		if err != nil {
			return "", fmt.Errorf("unsupported _id type %s: %w", v.Type, err)
		}
		return string(extJSON), nil
	}
}
