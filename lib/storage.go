package lib

import (
	"context"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	storage Storage
)

// Storage define all storage action and methods
type Storage struct {
	Database        string
	client          *mongo.Client
	timeoutDuration time.Duration
}

// IsNotFound resource not found
func IsNotFound(err error) bool {
	return err == mongo.ErrNoDocuments
}

// IsKeyDuplicated test if a key already exist on storage
func IsKeyDuplicated(err error) bool {
	return strings.Contains(err.Error(), "duplicate key error collection")
}

// IsDuplicateKey test if a key already exist on storage
func IsDuplicateKey(key string, err error) bool {
	return strings.Contains(err.Error(), "duplicate key error collection") &&
		strings.Contains(err.Error(), "index: "+key)

}

// New create new Storage Struct
func NewStorage(ctx context.Context, url string) (*Storage, error) {
	if storage.client != nil {
		return &storage, nil
	}

	client, err := GetMongoClient(ctx, url)
	if err != nil {
		return nil, err
	}

	storage = Storage{
		client:          client,
		timeoutDuration: 30 * time.Minute,
	}
	return &storage, nil
}

// GetMongoClient : To Get Mongo Client Object
func GetMongoClient(ctx context.Context, url string) (*mongo.Client, error) {
	client, err := mongo.NewClient(options.Client().ApplyURI(url))
	if err != nil {
		return nil, err
	}

	err = client.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return client, err
}

func (s *Storage) GetDatabase(name string) *mongo.Database {
	s.Database = name
	return s.client.Database(name)
}

func (s *Storage) GetCollection(name string) *mongo.Collection {
	return s.client.Database(s.Database).Collection(name)
}

func (s *Storage) Disconnect(ctx context.Context) error {
	return s.client.Disconnect(ctx)
}

func (s *Storage) Connect(ctx context.Context) error {
	return s.client.Connect(ctx)
}
