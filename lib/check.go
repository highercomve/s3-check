package lib

import (
	"context"
	"time"

	"github.com/spf13/cobra"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type ObjectsQuery struct {
	Ctx    context.Context
	Col    *mongo.Collection
	S3     *S3Client
	Limit  int64
	Filter bson.M
	Pages  int64
	Count  int64
}

type ObjectQuery struct {
	*ObjectsQuery
	Page int64
}

type QueryResult struct {
	Data   bson.M
	Filter bson.M
	Limit  int64
	Pages  int64
	Page   int64
	Count  int64
	Index  int64
	Err    error
}
type RChan chan QueryResult

func CheckStorage(cmd *cobra.Command, args []string) (err error) {
	var key string
	var secret string
	var region string
	var bucket string
	var endpoint string
	var connection string
	var database string
	var collection string

	if key, err = cmd.Flags().GetString("access_key"); err != nil {
		return err
	}
	if secret, err = cmd.Flags().GetString("secret"); err != nil {
		return err
	}
	if region, err = cmd.Flags().GetString("region"); err != nil {
		return err
	}
	if bucket, err = cmd.Flags().GetString("bucket"); err != nil {
		return err
	}
	if endpoint, err = cmd.Flags().GetString("endpoint"); err != nil {
		return err
	}
	if database, err = cmd.Flags().GetString("database"); err != nil {
		return err
	}
	if collection, err = cmd.Flags().GetString("collection"); err != nil {
		return err
	}
	if connection, err = cmd.Flags().GetString("connection"); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	storage, err := NewDbConnection(ctx, connection)
	if err != nil {
		return err
	}

	limit := int64(5)
	filter := bson.M{"sizeint": bson.M{"$gt": 0}}
	db := storage.GetDatabase(database)
	col := db.Collection(collection)

	s3params := &S3ConnParams{
		Key:      key,
		Secret:   secret,
		Region:   region,
		Bucket:   bucket,
		Endpoint: endpoint,
	}
	s3, err := NewS3Connect(ctx, s3params)
	if err != nil {
		return err
	}

	count, err := col.CountDocuments(ctx, filter)
	if err != nil {
		return err
	}

	err = getObjects(&ObjectsQuery{
		Ctx:   ctx,
		Count: count,
		Limit: limit,
		Col:   col,
		S3:    s3,
	})
	if err != nil {
		return err
	}

	return nil
}

func getObjects(query *ObjectsQuery) (err error) {
	query.Pages = query.Count / query.Limit
	resultC := make(RChan)

	for i := int64(0); i <= query.Pages; i++ {
		query := ObjectQuery{
			ObjectsQuery: query,
			Page:         i,
		}

		go func() {
			err := getPage(&query, resultC)
			if err != nil {
				close(resultC)
			}
		}()
	}

	results := bson.A{}
	for r := range resultC {
		if r.Err != nil {
			close(resultC)
			err = r.Err
			break
		}

		results = append(results, r.Data)
		if len(results) == int(query.Count) {
			close(resultC)
			break
		}
	}

	return err
}

func getPage(query *ObjectQuery, r RChan) error {
	options := &options.FindOptions{}
	options.SetLimit(query.Limit)
	options.SetSkip(query.Page * query.Limit)

	cursor, err := query.Col.Find(query.Ctx, query.Filter, options)
	if err != nil {
		result := QueryResult{Err: err}
		r <- result
	}

	for cursor.Next(query.Ctx) {
		result := QueryResult{
			Page:  query.Page,
			Pages: query.Pages,
			Limit: query.Limit,
			Count: query.Count,
		}
		object := bson.M{}
		if err = cursor.Decode(&object); err != nil {
			result.Err = err
			r <- result
		}
		result.Data = object
		r <- result
	}

	return err
}
