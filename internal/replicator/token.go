package replicator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// TokenStore persists and retrieves the MongoDB change-stream resume token.
// Each collection has its own TokenStore instance so tokens never collide.
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

// NewFileTokenStore creates a FileTokenStore for the given collection.
// The token file path is derived from basePath by inserting the collection
// name before the extension, e.g.:
//
//	"resume_token.json" + "orders"  →  "resume_token_orders.json"
//	"/data/tok"         + "orders"  →  "/data/tok_orders"
func NewFileTokenStore(basePath, collection string, logger *slog.Logger) *FileTokenStore {
	return &FileTokenStore{
		path:   collectionTokenPath(basePath, collection),
		logger: logger,
	}
}

// collectionTokenPath inserts the collection name into a base file path before
// the extension (or at the end when there is no extension).
func collectionTokenPath(basePath, collection string) string {
	ext := filepath.Ext(basePath)
	base := strings.TrimSuffix(basePath, ext)
	return fmt.Sprintf("%s_%s%s", base, collection, ext)
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

type mongoTokenDoc struct {
	ID      string    `bson:"_id"`
	Token   bson.Raw  `bson:"token"`
	SavedAt time.Time `bson:"saved_at"`
}

// MongoTokenStore persists the resume token as a document in a dedicated
// MongoDB collection.  Each watched collection's token is stored under a
// unique document ID so multiple collections can share the same metadata
// collection.
type MongoTokenStore struct {
	coll   *mongo.Collection
	docID  string // "token_<collection>" — unique per watched collection
	logger *slog.Logger
}

// NewMongoTokenStore creates a MongoTokenStore for the given collection.
// All per-collection tokens are stored as separate documents inside metaColl,
// keyed by "token_<collection>".
func NewMongoTokenStore(metaColl *mongo.Collection, collection string, logger *slog.Logger) *MongoTokenStore {
	return &MongoTokenStore{
		coll:   metaColl,
		docID:  "token_" + collection,
		logger: logger,
	}
}

// Save upserts the resume token into the metadata collection.
func (m *MongoTokenStore) Save(token bson.Raw) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	doc := mongoTokenDoc{
		ID:      m.docID,
		Token:   token,
		SavedAt: time.Now().UTC(),
	}
	filter := bson.D{{Key: "_id", Value: m.docID}}
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
	filter := bson.D{{Key: "_id", Value: m.docID}}
	err := m.coll.FindOne(ctx, filter).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		m.logger.Info("no resume token in metadata collection, starting from the beginning",
			"doc_id", m.docID)
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("token load: %w", err)
	}

	m.logger.Info("loaded resume token from MongoDB",
		"collection", m.coll.Name(), "doc_id", m.docID, "saved_at", doc.SavedAt)
	return doc.Token, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// S3TokenStore
// ─────────────────────────────────────────────────────────────────────────────

// S3TokenStore persists the resume token as a JSON object in an S3 bucket.
// Each collection's token is stored under its own S3 key:
//
//	<prefix><collection>.json
//
// AWS credentials are resolved via the standard SDK credential chain
// (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env vars, ~/.aws/credentials,
// EC2/ECS instance metadata, IAM roles for service accounts, etc.).
type S3TokenStore struct {
	client     *s3.Client
	bucket     string
	key        string // full object key, e.g. "mongo-elastic/orders.json"
	logger     *slog.Logger
}

// NewS3TokenStore creates an S3TokenStore for the given collection.
// region may be empty to use the SDK default (AWS_DEFAULT_REGION env var,
// ~/.aws/config, or EC2 metadata).
func NewS3TokenStore(ctx context.Context, bucket, prefix, region, collection string, logger *slog.Logger) (*S3TokenStore, error) {
	var opts []func(*awsconfig.LoadOptions) error
	if region != "" {
		opts = append(opts, awsconfig.WithRegion(region))
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	return &S3TokenStore{
		client: s3.NewFromConfig(cfg),
		bucket: bucket,
		key:    prefix + collection + ".json",
		logger: logger,
	}, nil
}

// Save writes the resume token to S3 as a JSON object.
func (s *S3TokenStore) Save(token bson.Raw) error {
	doc := fileTokenDoc{
		SavedAt: time.Now().UTC(),
		Token:   []byte(token),
	}
	data, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("token marshal: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(s.key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/json"),
	})
	if err != nil {
		return fmt.Errorf("token s3 put: %w", err)
	}
	return nil
}

// Load reads the resume token from S3. Returns (nil, nil) when the object does
// not exist yet (fresh start or first run against this bucket/prefix).
func (s *S3TokenStore) Load() (bson.Raw, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key),
	})
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			s.logger.Info("no resume token in S3, starting from the beginning",
				"bucket", s.bucket, "key", s.key)
			return nil, nil
		}
		return nil, fmt.Errorf("token s3 get: %w", err)
	}
	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("token s3 read body: %w", err)
	}

	var doc fileTokenDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		s.logger.Warn("corrupt S3 token object, ignoring and starting fresh",
			"bucket", s.bucket, "key", s.key, "error", err)
		return nil, nil
	}

	s.logger.Info("loaded resume token from S3",
		"bucket", s.bucket, "key", s.key, "saved_at", doc.SavedAt)
	return bson.Raw(doc.Token), nil
}
