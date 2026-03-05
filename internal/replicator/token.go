package replicator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// TokenStore persists and retrieves the MongoDB change-stream resume token.
// Implementations must be safe to call concurrently from a single goroutine.
type TokenStore interface {
	// Save persists the resume token returned by ChangeStream.ResumeToken().
	Save(token bson.Raw) error
	// Load retrieves the last saved resume token, or (nil, nil) when none exists.
	Load() (bson.Raw, error)
}

// ─────────────────────────────────────────────────────────────────────────────
// FileTokenStore
// ─────────────────────────────────────────────────────────────────────────────

// fileTokenDoc is the on-disk JSON envelope.
// Token is stored as raw BSON bytes (base64-encoded by encoding/json).
type fileTokenDoc struct {
	SavedAt time.Time `json:"saved_at"`
	Token   []byte    `json:"token"`
}

// FileTokenStore persists the resume token in a local JSON file.
type FileTokenStore struct {
	path   string
	logger *slog.Logger
}

// NewFileTokenStore creates a FileTokenStore that reads/writes at path.
func NewFileTokenStore(path string, logger *slog.Logger) *FileTokenStore {
	return &FileTokenStore{path: path, logger: logger}
}

// Save writes the resume token to disk atomically (write-then-rename).
func (f *FileTokenStore) Save(token bson.Raw) error {
	doc := fileTokenDoc{
		SavedAt: time.Now().UTC(),
		Token:   []byte(token),
	}
	data, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("token marshal: %w", err)
	}

	// Write to a temp file first, then rename for atomicity.
	tmp := f.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("token write: %w", err)
	}
	if err := os.Rename(tmp, f.path); err != nil {
		return fmt.Errorf("token rename: %w", err)
	}
	return nil
}

// Load reads the resume token from disk. Returns (nil, nil) when the file
// does not exist yet (fresh start).
func (f *FileTokenStore) Load() (bson.Raw, error) {
	data, err := os.ReadFile(f.path)
	if errors.Is(err, os.ErrNotExist) {
		f.logger.Info("no resume token file found, starting from the beginning", "path", f.path)
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("token read: %w", err)
	}

	var doc fileTokenDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		f.logger.Warn("corrupt token file, ignoring and starting fresh", "path", f.path, "error", err)
		return nil, nil
	}

	f.logger.Info("loaded resume token from file", "path", f.path, "saved_at", doc.SavedAt)
	return bson.Raw(doc.Token), nil
}

// ─────────────────────────────────────────────────────────────────────────────
// MongoTokenStore
// ─────────────────────────────────────────────────────────────────────────────

const metaDocID = "change_stream_token"

type mongoTokenDoc struct {
	ID      string    `bson:"_id"`
	Token   bson.Raw  `bson:"token"`
	SavedAt time.Time `bson:"saved_at"`
}

// MongoTokenStore persists the resume token in a dedicated MongoDB collection.
// This is the preferred option when the service runs in a stateless container.
type MongoTokenStore struct {
	coll   *mongo.Collection
	logger *slog.Logger
}

// NewMongoTokenStore creates a MongoTokenStore backed by coll.
func NewMongoTokenStore(coll *mongo.Collection, logger *slog.Logger) *MongoTokenStore {
	return &MongoTokenStore{coll: coll, logger: logger}
}

// Save upserts the resume token into the metadata collection.
func (m *MongoTokenStore) Save(token bson.Raw) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	doc := mongoTokenDoc{
		ID:      metaDocID,
		Token:   token,
		SavedAt: time.Now().UTC(),
	}
	filter := bson.D{{Key: "_id", Value: metaDocID}}
	update := bson.D{{Key: "$set", Value: doc}}
	opts := options.Update().SetUpsert(true)

	if _, err := m.coll.UpdateOne(ctx, filter, update, opts); err != nil {
		return fmt.Errorf("token upsert: %w", err)
	}
	return nil
}

// Load retrieves the last saved resume token. Returns (nil, nil) on first run.
func (m *MongoTokenStore) Load() (bson.Raw, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var doc mongoTokenDoc
	filter := bson.D{{Key: "_id", Value: metaDocID}}
	err := m.coll.FindOne(ctx, filter).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		m.logger.Info("no resume token in metadata collection, starting from the beginning")
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("token load: %w", err)
	}

	m.logger.Info("loaded resume token from MongoDB", "collection", m.coll.Name(), "saved_at", doc.SavedAt)
	return doc.Token, nil
}
