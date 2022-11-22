package lib

import (
	"context"
	"fmt"
	"log"

	"github.com/spf13/cobra"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type ObjectsQuery struct {
	Col      *mongo.Collection
	S3Params *S3ConnParams
	Limit    int64
	Filter   bson.M
	Pages    int64
	Count    int64
}

type ObjectQuery struct {
	*ObjectsQuery
	Page int64
}

type ObjectResult struct {
	Data bson.M
	Err  error
}

type ReaderChannel chan ObjectResult
type WriterChannel chan ObjectResult
type SearcherChannel chan ObjectResult

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

	ctx, cancel := context.WithCancel(context.Background())
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

	count, err := col.CountDocuments(ctx, filter)
	if err != nil {
		return err
	}

	err = getMissingObjects(ctx, &ObjectsQuery{
		Count:    count,
		Limit:    limit,
		Col:      col,
		S3Params: s3params,
	})
	if err != nil {
		return err
	}

	return nil
}

func SearchS3(ctx context.Context, s3params *S3ConnParams, s SearcherChannel, w WriterChannel) {
	s3, err := NewS3Connect(ctx, s3params)
	if err != nil {
		log.Fatal(err)
	}

	for r := range s {
		fmt.Printf("%+v \n", r)
		if r.Err != nil {
			return
		}

		id, ok := r.Data["id"]
		if !ok {
			return
		}
		exist, err := s3.ObjectExist(ctx, id.(string))
		if err != nil {
			return
		}
		if !exist {
			w <- r
		}
	}
}

func Writter(ctx context.Context, writer WriterChannel) {
	for r := range writer {
		fmt.Printf("%+v \n", r)
	}
}

func getMissingObjects(ctx context.Context, query *ObjectsQuery) (err error) {
	query.Pages = query.Count / query.Limit
	reader := make(ReaderChannel)
	writter := make(WriterChannel)
	searcher := make(SearcherChannel)

	// go SearchS3(ctx, query.S3Params, searcher, writter)
	// go Writter(ctx, writter)

	fmt.Printf("here")
	for i := int64(0); i <= query.Pages; i++ {
		query := ObjectQuery{
			ObjectsQuery: query,
			Page:         i,
		}

		go func(ctx context.Context) {
			fmt.Printf("%+v \n", query.ObjectsQuery)
			err := getPage(ctx, &query, reader)
			if err != nil {
				close(reader)
				close(writter)
				close(searcher)
			}
		}(ctx)
	}

	results := 0
	for r := range reader {
		fmt.Printf("%v", r)
		results++
		searcher <- r
		if r.Err != nil || results >= int(query.Count) {
			break
		}
	}

	close(reader)
	close(writter)
	close(searcher)

	return err
}

func getPage(ctx context.Context, query *ObjectQuery, r ReaderChannel) error {
	options := &options.FindOptions{}
	options.SetLimit(query.Limit)
	options.SetSkip(query.Page * query.Limit)

	fmt.Printf("%+v \n", query.ObjectsQuery)
	cursor, err := query.Col.Find(ctx, query.Filter, options)
	if err != nil {
		result := ObjectResult{Err: err}
		r <- result
	}

	for cursor.Next(context.TODO()) {
		result := ObjectResult{}
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
