// Package config loads and validates service configuration from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// TokenStoreType selects where the resume token is persisted.
type TokenStoreType string

const (
	TokenStoreFile  TokenStoreType = "file"
	TokenStoreMongo TokenStoreType = "mongo"
)

// CollectionMapping pairs a MongoDB collection name with its target
// Elasticsearch index name.
type CollectionMapping struct {
	Collection string // MongoDB collection name
	Index      string // Elasticsearch index name (defaults to Collection)
}

// Config holds all runtime configuration for the replicator.
type Config struct {
	// MongoDB
	MongoURI    string
	MongoDB     string
	Collections []CollectionMapping // one entry per watched collection

	// Elasticsearch
	ESAddresses []string
	ESUsername  string
	ESPassword  string

	// Resume-token storage
	TokenStore     TokenStoreType
	TokenFilePath  string // base path when TokenStore == "file"
	TokenMongoMeta string // metadata collection name when TokenStore == "mongo"

	// Retry / backoff
	MaxRetries     int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration

	// Change-stream tuning
	// How many events to batch before flushing the resume token on disk.
	// Set to 1 for maximum durability (token saved after every event).
	TokenSaveInterval int

	// Full-sync options
	//
	// StartupFullSync (STARTUP_FULL_SYNC) forces a complete MongoDB→ES
	// collection scan on the first startup iteration, before the change stream
	// opens.  Safe because all operations are idempotent upserts.
	StartupFullSync bool

	// StaleTokenResync (STALE_TOKEN_RESYNC) enables automatic full-resync when
	// the saved resume token is no longer in the oplog.
	// Default: true (recommended).
	StaleTokenResync bool

	// SyncBatchSize (SYNC_BATCH_SIZE) is the number of documents sent per
	// Elasticsearch bulk request during a full sync (default 500).
	SyncBatchSize int
}

// Load reads configuration from environment variables, applying defaults.
//
// Collection mappings are specified via COLLECTIONS as a comma-separated list
// of "collection:index" pairs.  The index part is optional and defaults to the
// collection name when omitted:
//
//	COLLECTIONS=orders:orders_index,products,users:user_profiles
//
// For single-collection deployments the legacy MONGO_COLLECTION / ES_INDEX
// variables are still accepted as a fallback when COLLECTIONS is not set.
func Load() (*Config, error) {
	cfg := &Config{
		MongoURI:          getEnv("MONGO_URI", "mongodb://localhost:27017"),
		MongoDB:           getEnv("MONGO_DB", "mydb"),
		ESAddresses:       splitCSV(getEnv("ES_ADDRESSES", "http://localhost:9200")),
		ESUsername:        getEnv("ES_USERNAME", ""),
		ESPassword:        getEnv("ES_PASSWORD", ""),
		TokenStore:        TokenStoreType(getEnv("TOKEN_STORE", string(TokenStoreFile))),
		TokenFilePath:     getEnv("TOKEN_FILE", "resume_token.json"),
		TokenMongoMeta:    getEnv("TOKEN_MONGO_META_COLLECTION", "_replicator_meta"),
		MaxRetries:        getEnvInt("MAX_RETRIES", 10),
		InitialBackoff:    getEnvDuration("INITIAL_BACKOFF", 500*time.Millisecond),
		MaxBackoff:        getEnvDuration("MAX_BACKOFF", 30*time.Second),
		TokenSaveInterval: getEnvInt("TOKEN_SAVE_INTERVAL", 1),
		StartupFullSync:   getEnvBool("STARTUP_FULL_SYNC", false),
		StaleTokenResync:  getEnvBool("STALE_TOKEN_RESYNC", true),
		SyncBatchSize:     getEnvInt("SYNC_BATCH_SIZE", 500),
	}

	// ── Resolve collection mappings ───────────────────────────────────────────
	if raw := os.Getenv("COLLECTIONS"); raw != "" {
		cfg.Collections = parseCollections(raw)
	} else {
		// Legacy single-collection fallback.
		col := getEnv("MONGO_COLLECTION", "mycollection")
		idx := getEnv("ES_INDEX", col)
		cfg.Collections = []CollectionMapping{{Collection: col, Index: idx}}
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// parseCollections parses a comma-separated list of "collection[:index]" pairs.
// When the index part is absent the collection name is used as the index name.
func parseCollections(raw string) []CollectionMapping {
	var out []CollectionMapping
	for _, part := range splitCSV(raw) {
		if i := strings.IndexByte(part, ':'); i >= 0 {
			col := strings.TrimSpace(part[:i])
			idx := strings.TrimSpace(part[i+1:])
			if idx == "" {
				idx = col
			}
			out = append(out, CollectionMapping{Collection: col, Index: idx})
		} else {
			name := strings.TrimSpace(part)
			out = append(out, CollectionMapping{Collection: name, Index: name})
		}
	}
	return out
}

func (c *Config) validate() error {
	if c.MongoURI == "" {
		return fmt.Errorf("MONGO_URI must not be empty")
	}
	if c.MongoDB == "" {
		return fmt.Errorf("MONGO_DB must not be empty")
	}
	if len(c.Collections) == 0 {
		return fmt.Errorf("at least one collection must be configured via COLLECTIONS or MONGO_COLLECTION")
	}
	for i, m := range c.Collections {
		if m.Collection == "" {
			return fmt.Errorf("collections[%d]: collection name must not be empty", i)
		}
		if m.Index == "" {
			return fmt.Errorf("collections[%d]: index name must not be empty", i)
		}
	}
	if len(c.ESAddresses) == 0 {
		return fmt.Errorf("ES_ADDRESSES must not be empty")
	}
	if c.TokenStore != TokenStoreFile && c.TokenStore != TokenStoreMongo {
		return fmt.Errorf("TOKEN_STORE must be 'file' or 'mongo', got %q", c.TokenStore)
	}
	if c.MaxRetries < 0 {
		return fmt.Errorf("MAX_RETRIES must be >= 0")
	}
	return nil
}

// --- helpers -----------------------------------------------------------------

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func splitCSV(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if t := strings.TrimSpace(part); t != "" {
			out = append(out, t)
		}
	}
	return out
}

func getEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func getEnvBool(key string, def bool) bool {
	v := os.Getenv(key)
	switch v {
	case "true", "1", "yes":
		return true
	case "false", "0", "no":
		return false
	default:
		return def
	}
}

func getEnvDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
