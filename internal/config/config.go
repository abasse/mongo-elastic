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

// Config holds all runtime configuration for the replicator.
type Config struct {
	// MongoDB
	MongoURI        string
	MongoDB         string
	MongoCollection string

	// Elasticsearch
	ESAddresses []string
	ESUsername  string
	ESPassword  string
	ESIndex     string

	// Resume-token storage
	TokenStore     TokenStoreType
	TokenFilePath  string // used when TokenStore == "file"
	TokenMongoMeta string // metadata collection name, used when TokenStore == "mongo"

	// Retry / backoff
	MaxRetries     int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration

	// Change-stream tuning
	// How many events to batch before flushing the resume token on disk.
	// Set to 1 for maximum durability (token saved after every event).
	TokenSaveInterval int
}

// Load reads configuration from environment variables, applying defaults.
func Load() (*Config, error) {
	cfg := &Config{
		MongoURI:          getEnv("MONGO_URI", "mongodb://localhost:27017"),
		MongoDB:           getEnv("MONGO_DB", "mydb"),
		MongoCollection:   getEnv("MONGO_COLLECTION", "mycollection"),
		ESAddresses:       splitCSV(getEnv("ES_ADDRESSES", "http://localhost:9200")),
		ESUsername:        getEnv("ES_USERNAME", ""),
		ESPassword:        getEnv("ES_PASSWORD", ""),
		ESIndex:           getEnv("ES_INDEX", ""),
		TokenStore:        TokenStoreType(getEnv("TOKEN_STORE", string(TokenStoreFile))),
		TokenFilePath:     getEnv("TOKEN_FILE", "resume_token.json"),
		TokenMongoMeta:    getEnv("TOKEN_MONGO_META_COLLECTION", "_replicator_meta"),
		MaxRetries:        getEnvInt("MAX_RETRIES", 10),
		InitialBackoff:    getEnvDuration("INITIAL_BACKOFF", 500*time.Millisecond),
		MaxBackoff:        getEnvDuration("MAX_BACKOFF", 30*time.Second),
		TokenSaveInterval: getEnvInt("TOKEN_SAVE_INTERVAL", 1),
	}

	// Default ES index to collection name when not explicitly set.
	if cfg.ESIndex == "" {
		cfg.ESIndex = cfg.MongoCollection
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c *Config) validate() error {
	if c.MongoURI == "" {
		return fmt.Errorf("MONGO_URI must not be empty")
	}
	if c.MongoDB == "" {
		return fmt.Errorf("MONGO_DB must not be empty")
	}
	if c.MongoCollection == "" {
		return fmt.Errorf("MONGO_COLLECTION must not be empty")
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

func getEnvDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
