package replicator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/abasse/mongo-elastic/internal/config"
)

// collectionWorker handles replication for a single MongoDB collection →
// Elasticsearch index pair.  Multiple workers run concurrently inside a
// Replicator, one per configured CollectionMapping.
type collectionWorker struct {
	mapping     config.CollectionMapping
	mongoColl   *mongo.Collection
	mongoClient *mongo.Client // shared, used for cluster-time queries
	esClient    *elasticsearch.Client
	tokenStore  TokenStore
	retryCfg    RetryConfig
	cfg         *config.Config
	logger      *slog.Logger // pre-scoped with collection / index fields

	// postSyncClusterTime is set by FullSync so that the next runOnce opens
	// the change stream with SetStartAtOperationTime instead of SetResumeAfter,
	// guaranteeing no gap between the scan and the first live event.
	// runOnce clears the field after consuming it.
	postSyncClusterTime *primitive.Timestamp
}

// ─────────────────────────────────────────────────────────────────────────────
// Main loop
// ─────────────────────────────────────────────────────────────────────────────

// run is the worker's main loop.  It handles startup full-sync, opens the
// change stream, processes events with retry, and reconnects automatically.
// It returns only when ctx is cancelled or a non-recoverable error occurs.
func (w *collectionWorker) run(ctx context.Context) error {
	firstRun := true

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		if firstRun && w.cfg.StartupFullSync {
			w.logger.Info("startup full sync requested")
			if err := w.FullSync(ctx); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return ctx.Err()
				}
				w.logger.Error("startup full sync failed, will retry after backoff", "error", err)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(backoffDelay(w.cfg.InitialBackoff, w.cfg.MaxBackoff, 0)):
				}
				continue
			}
		}
		firstRun = false

		err := w.runOnce(ctx)
		if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return ctx.Err()
		}

		if isStaleResumeTokenError(err) {
			if !w.cfg.StaleTokenResync {
				w.logger.Error("resume token is stale and STALE_TOKEN_RESYNC=false — stopping worker",
					"error", err,
					"hint", "set STALE_TOKEN_RESYNC=true or STARTUP_FULL_SYNC=true to recover automatically",
				)
				return fmt.Errorf("stale resume token (automatic resync disabled): %w", err)
			}
			w.logger.Warn("resume token is no longer in the oplog — triggering full sync to recover",
				"error", err)
			if syncErr := w.FullSync(ctx); syncErr != nil {
				if errors.Is(syncErr, context.Canceled) || errors.Is(syncErr, context.DeadlineExceeded) {
					return ctx.Err()
				}
				w.logger.Error("full sync after stale token failed", "error", syncErr)
			}
		} else {
			w.logger.Error("replication cycle ended with error, restarting", "error", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoffDelay(w.cfg.InitialBackoff, w.cfg.MaxBackoff, 0)):
		}
	}
}

// runOnce opens a single change-stream session and processes events until the
// context is cancelled or an error occurs.
func (w *collectionWorker) runOnce(ctx context.Context) error {
	// ── Determine stream start position ──────────────────────────────────────
	// Priority:
	//   1. postSyncClusterTime (FullSync just ran)    → SetStartAtOperationTime
	//   2. Persisted sync-marker token               → SetStartAtOperationTime
	//   3. Persisted real change-stream token        → SetResumeAfter
	//   4. Nothing                                   → start from "now"

	csOpts := options.ChangeStream().
		SetFullDocument(options.UpdateLookup).
		SetMaxAwaitTime(10 * time.Second)

	if w.postSyncClusterTime != nil {
		ts := w.postSyncClusterTime
		w.postSyncClusterTime = nil
		csOpts.SetStartAtOperationTime(ts)
		w.logger.Info("opening change stream at post-sync cluster time", "t", ts.T, "i", ts.I)
	} else {
		token, err := w.tokenStore.Load()
		if err != nil {
			w.logger.Warn("could not load resume token, starting from now", "error", err)
		}
		switch {
		case token != nil && isSyncMarkerToken(token):
			if ts, ok := clusterTimeFromSyncToken(token); ok {
				csOpts.SetStartAtOperationTime(ts)
				w.logger.Info("opening change stream at persisted cluster time (sync marker)",
					"t", ts.T, "i", ts.I)
			} else {
				w.logger.Warn("sync marker token has no cluster time, starting from now")
			}
		case token != nil:
			csOpts.SetResumeAfter(token)
			w.logger.Info("resuming change stream from saved token")
		default:
			w.logger.Info("opening change stream from the current cluster time (no token)")
		}
	}

	pipeline := mongo.Pipeline{
		bson.D{{Key: "$match", Value: bson.D{
			{Key: "operationType", Value: bson.D{
				{Key: "$in", Value: bson.A{"insert", "update", "replace", "delete"}},
			}},
		}}},
	}

	cs, err := w.mongoColl.Watch(ctx, pipeline, csOpts)
	if err != nil {
		return fmt.Errorf("watch: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if cerr := cs.Close(closeCtx); cerr != nil {
			w.logger.Warn("error closing change stream", "error", cerr)
		}
	}()

	w.logger.Info("change stream opened")

	eventCount := 0
	for cs.Next(ctx) {
		var event changeEvent
		if err := cs.Decode(&event); err != nil {
			return fmt.Errorf("decode event: %w", err)
		}

		log := w.logger.With(
			"operationType", event.OperationType,
			"ns", fmt.Sprintf("%s.%s", event.NS.DB, event.NS.Coll),
		)

		err := withRetry(ctx, w.retryCfg, log, "process event", func() error {
			return w.processEvent(ctx, &event, log)
		})
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			log.Error("failed to process event after retries, skipping", "error", err)
		}

		eventCount++
		if eventCount%w.cfg.TokenSaveInterval == 0 {
			if serr := w.tokenStore.Save(cs.ResumeToken()); serr != nil {
				log.Warn("failed to save resume token", "error", serr)
			}
		}
	}

	if err := cs.Err(); err != nil {
		return fmt.Errorf("change stream error: %w", err)
	}
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Event processing
// ─────────────────────────────────────────────────────────────────────────────

func (w *collectionWorker) processEvent(ctx context.Context, event *changeEvent, log *slog.Logger) error {
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
		return w.indexDocument(ctx, docID, event.FullDocument, log)

	case "update":
		if event.FullDocument != nil {
			return w.indexDocument(ctx, docID, event.FullDocument, log)
		}
		if event.UpdateDescription != nil {
			return w.partialUpdate(ctx, docID, event.UpdateDescription, log)
		}
		log.Warn("update event has neither fullDocument nor updateDescription, skipping")
		return nil

	case "delete":
		return w.deleteDocument(ctx, docID, log)

	default:
		log.Warn("unhandled operation type, skipping", "operationType", event.OperationType)
		return nil
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Elasticsearch helpers
// ─────────────────────────────────────────────────────────────────────────────

func (w *collectionWorker) indexDocument(ctx context.Context, id string, doc bson.Raw, log *slog.Logger) error {
	body, err := bsonToJSON(doc)
	if err != nil {
		return fmt.Errorf("bson→json: %w", err)
	}

	req := esapi.IndexRequest{
		Index:      w.mapping.Index,
		DocumentID: id,
		Body:       bytes.NewReader(body),
		Refresh:    "false",
	}
	res, err := req.Do(ctx, w.esClient)
	if err != nil {
		return fmt.Errorf("ES index request: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		errBody, _ := io.ReadAll(res.Body)
		return fmt.Errorf("ES index %s: %s — %s", id, res.Status(), errBody)
	}

	log.Info("document indexed", "es_id", id, "index", w.mapping.Index)
	return nil
}

func (w *collectionWorker) partialUpdate(ctx context.Context, id string, ud *updateDescription, log *slog.Logger) error {
	updatedJSON, err := bsonToJSON(ud.UpdatedFields)
	if err != nil {
		return fmt.Errorf("updatedFields bson→json: %w", err)
	}

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

	body := fmt.Sprintf(`{"doc":%s,"doc_as_upsert":true}`, patchJSON)
	req := esapi.UpdateRequest{
		Index:      w.mapping.Index,
		DocumentID: id,
		Body:       bytes.NewBufferString(body),
		Refresh:    "false",
	}
	res, err := req.Do(ctx, w.esClient)
	if err != nil {
		return fmt.Errorf("ES update request: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		errBody, _ := io.ReadAll(res.Body)
		return fmt.Errorf("ES update %s: %s — %s", id, res.Status(), errBody)
	}

	log.Info("document partially updated", "es_id", id, "index", w.mapping.Index,
		"removed_fields", ud.RemovedFields)
	return nil
}

func (w *collectionWorker) deleteDocument(ctx context.Context, id string, log *slog.Logger) error {
	req := esapi.DeleteRequest{
		Index:      w.mapping.Index,
		DocumentID: id,
		Refresh:    "false",
	}
	res, err := req.Do(ctx, w.esClient)
	if err != nil {
		return fmt.Errorf("ES delete request: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode == 404 {
		log.Warn("document not found in ES during delete (already absent)", "es_id", id)
		return nil
	}
	if res.IsError() {
		errBody, _ := io.ReadAll(res.Body)
		return fmt.Errorf("ES delete %s: %s — %s", id, res.Status(), errBody)
	}

	log.Info("document deleted", "es_id", id, "index", w.mapping.Index)
	return nil
}
