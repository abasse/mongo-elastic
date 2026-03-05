package replicator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// FullSync performs a complete collection scan and bulk-upserts every document
// into Elasticsearch.  It is safe to run at any time because all Elasticsearch
// operations are idempotent (index with explicit _id).
//
// The flow is:
//  1. Capture the current MongoDB cluster timestamp T0.
//  2. Scan every document in the collection and bulk-upsert into ES.
//  3. Store T0 as a synthetic "sync marker" token so that the subsequent
//     change stream opens with SetStartAtOperationTime(T0).  Any events
//     that arrived during the scan (T0 → scan-end) will be replayed once,
//     but because upsert/delete are idempotent this is harmless.
func (r *Replicator) FullSync(ctx context.Context) error {
	start := time.Now()
	log := r.logger.With(
		slog.String("db", r.cfg.MongoDB),
		slog.String("collection", r.cfg.MongoCollection),
		slog.String("index", r.cfg.ESIndex),
		slog.Int("batch_size", r.cfg.SyncBatchSize),
	)
	log.Info("full sync starting")

	// ── 1. Capture cluster time BEFORE the scan ───────────────────────────────
	clusterTS, err := r.currentClusterTime(ctx)
	if err != nil {
		return fmt.Errorf("full sync: get cluster time: %w", err)
	}
	log.Info("cluster time captured", "t", clusterTS.T, "i", clusterTS.I)

	// ── 2. Iterate the collection ─────────────────────────────────────────────
	findOpts := options.Find().
		SetBatchSize(int32(r.cfg.SyncBatchSize)).
		SetNoCursorTimeout(false)

	cursor, err := r.mongoColl.Find(ctx, bson.D{}, findOpts)
	if err != nil {
		return fmt.Errorf("full sync: find: %w", err)
	}
	defer cursor.Close(ctx)

	var (
		batch     []bulkItem
		total     int
		succeeded int
		failed    int
	)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		ok, ko, err := r.bulkIndex(ctx, batch)
		succeeded += ok
		failed += ko
		batch = batch[:0]
		return err
	}

	for cursor.Next(ctx) {
		var raw bson.Raw
		if err := cursor.Decode(&raw); err != nil {
			log.Warn("decode error, skipping document", "error", err)
			continue
		}

		idVal, err := raw.LookupErr("_id")
		if err != nil {
			log.Warn("document has no _id, skipping")
			continue
		}
		docID, err := rawValueToString(idVal)
		if err != nil {
			log.Warn("cannot stringify _id, skipping", "error", err)
			continue
		}

		docJSON, err := bsonToJSON(raw)
		if err != nil {
			log.Warn("bson→json error, skipping", "id", docID, "error", err)
			continue
		}

		batch = append(batch, bulkItem{id: docID, body: docJSON})
		total++

		if len(batch) >= r.cfg.SyncBatchSize {
			if err := flush(); err != nil {
				return fmt.Errorf("full sync: bulk flush: %w", err)
			}
			log.Info("full sync progress", "synced", succeeded, "failed", failed)
		}
	}

	if err := cursor.Err(); err != nil {
		return fmt.Errorf("full sync: cursor: %w", err)
	}
	if err := flush(); err != nil {
		return fmt.Errorf("full sync: final flush: %w", err)
	}

	// ── 3. Save the sync-marker token so the next stream uses startAtOpTime ───
	markerToken, tokenErr := buildSyncMarkerToken(clusterTS)
	if tokenErr != nil {
		log.Warn("could not encode sync-marker token; stream will start from 'now'", "error", tokenErr)
	} else {
		if serr := r.tokenStore.Save(markerToken); serr != nil {
			log.Warn("could not save sync-marker token", "error", serr)
		}
		r.postSyncClusterTime = clusterTS
	}

	log.Info("full sync complete",
		"total_scanned", total,
		"succeeded", succeeded,
		"failed", failed,
		"elapsed", time.Since(start).String(),
	)
	if failed > 0 {
		log.Warn("some documents could not be indexed; they will be retried on next full sync",
			"failed_count", failed)
	}
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Bulk indexing
// ─────────────────────────────────────────────────────────────────────────────

type bulkItem struct {
	id   string
	body []byte
}

// bulkIndex sends a single Elasticsearch bulk request for all items.
// Per-document failures are logged but do not cause a fatal error.
// Returns (succeeded, failed, fatalErr).
func (r *Replicator) bulkIndex(ctx context.Context, items []bulkItem) (succeeded, failed int, _ error) {
	if len(items) == 0 {
		return 0, 0, nil
	}

	// Build NDJSON bulk body: action line + source line per document.
	var buf bytes.Buffer
	for _, item := range items {
		fmt.Fprintf(&buf, `{"index":{"_id":%q}}`+"\n", item.id)
		buf.Write(item.body)
		buf.WriteByte('\n')
	}

	res, err := r.esClient.Bulk(
		bytes.NewReader(buf.Bytes()),
		r.esClient.Bulk.WithContext(ctx),
		r.esClient.Bulk.WithIndex(r.cfg.ESIndex),
	)
	if err != nil {
		return 0, len(items), fmt.Errorf("bulk transport: %w", err)
	}
	defer res.Body.Close()

	body, _ := io.ReadAll(res.Body)

	if res.IsError() {
		return 0, len(items), fmt.Errorf("bulk HTTP %s: %s", res.Status(), body)
	}

	// Parse per-item results.
	var bulkResp struct {
		Errors bool `json:"errors"`
		Items  []map[string]struct {
			ID     string `json:"_id"`
			Status int    `json:"status"`
			Error  *struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
			} `json:"error"`
		} `json:"items"`
	}
	if err := json.Unmarshal(body, &bulkResp); err != nil {
		// Cannot parse — assume all succeeded (transport-level check already passed).
		r.logger.Warn("could not parse bulk response", "error", err)
		return len(items), 0, nil
	}

	for _, item := range bulkResp.Items {
		for action, result := range item {
			if result.Error != nil {
				failed++
				r.logger.Error("bulk item failed",
					"action", action,
					"id", result.ID,
					"status", result.Status,
					"type", result.Error.Type,
					"reason", result.Error.Reason,
				)
			} else {
				succeeded++
			}
		}
	}
	return succeeded, failed, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Cluster-time helpers
// ─────────────────────────────────────────────────────────────────────────────

// currentClusterTime returns the server's cluster time via the lightweight
// "hello" command. Falls back to the local wall clock when the server response
// doesn't include operationTime (e.g. standalone nodes without sessions).
func (r *Replicator) currentClusterTime(ctx context.Context) (*primitive.Timestamp, error) {
	var result bson.Raw
	err := r.mongoClient.
		Database("admin").
		RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).
		Decode(&result)

	if err == nil {
		if t, i, ok := result.Lookup("operationTime").TimestampOK(); ok {
			return &primitive.Timestamp{T: t, I: i}, nil
		}
	}

	r.logger.Warn("cannot read cluster time from server, using wall clock", "error", err)
	return &primitive.Timestamp{T: uint32(time.Now().Unix()), I: 0}, nil
}

// buildSyncMarkerToken creates a synthetic BSON resume token that encodes a
// cluster timestamp.  runOnce distinguishes this from a real change-stream
// token and calls SetStartAtOperationTime instead of SetResumeAfter.
func buildSyncMarkerToken(ts *primitive.Timestamp) (bson.Raw, error) {
	raw, err := bson.Marshal(bson.D{
		{Key: "_syncMarker", Value: true},
		{Key: "clusterTime", Value: *ts},
	})
	if err != nil {
		return nil, err
	}
	return bson.Raw(raw), nil
}

// isSyncMarkerToken reports whether token was produced by buildSyncMarkerToken.
func isSyncMarkerToken(token bson.Raw) bool {
	if token == nil {
		return false
	}
	v, err := token.LookupErr("_syncMarker")
	if err != nil {
		return false
	}
	b, ok := v.BooleanOK()
	return ok && b
}

// clusterTimeFromSyncToken extracts the timestamp embedded in a sync-marker
// token.
func clusterTimeFromSyncToken(token bson.Raw) (*primitive.Timestamp, bool) {
	v, err := token.LookupErr("clusterTime")
	if err != nil {
		return nil, false
	}
	t, i, ok := v.TimestampOK()
	if !ok {
		return nil, false
	}
	return &primitive.Timestamp{T: t, I: i}, true
}
