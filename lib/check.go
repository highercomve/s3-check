package lib

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

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

type ObjectData struct {
	ID string `json:"id" bson:"_id"`
}

type ObjectResult struct {
	Data ObjectData
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

func getMissingObjects(ctx context.Context, query *ObjectsQuery) (err error) {
	query.Pages = query.Count / query.Limit
	reader := make(ReaderChannel)
	writter := make(WriterChannel)
	searcher := make(SearcherChannel)
	errs := make(chan string)

	go func() {
		searchS3(ctx, query.S3Params, searcher, writter, errs)
	}()
	go func() {
		writeResult(ctx, writter, errs)
	}()

	for i := int64(0); i <= query.Pages; i++ {
		query := ObjectQuery{
			ObjectsQuery: query,
			Page:         i,
		}

		go func(ctx context.Context) {
			defer func() {
				if r := recover(); r != nil {
					return
				}
			}()
			err = getPage(ctx, &query, reader, errs)
			if err != nil {
				log.Fatal(err)
			}
		}(ctx)
	}

	results := 0
loop:
	for {
		select {
		case e, open := <-errs:
			if !open {
				break loop
			}
			err = fmt.Errorf(e)
			break loop
		case r, open := <-reader:
			if !open {
				break loop
			}
			results++
			searcher <- r

			if results >= int(query.Count) {
				break loop
			}
		}
	}

	close(reader)
	close(writter)
	close(searcher)
	close(errs)

	if err != nil {
		log.Fatal(err)
	}

	return nil
}

func searchS3(
	ctx context.Context,
	s3params *S3ConnParams,
	s SearcherChannel,
	w WriterChannel,
	errs chan string,
) {
	s3, err := NewS3Connect(ctx, s3params)
	if err != nil {
		log.Fatal(err)
	}

	for {
		select {
		case r, open := <-s:
			if !open {
				return
			}

			exist, err := s3.ObjectExist(ctx, r.Data.ID)
			if err != nil {
				log.Fatal(err)
			}

			if !exist {
				w <- r
			}
		}
	}
}

func writeResult(ctx context.Context, writer WriterChannel, errs chan string) {
	for {
		select {
		case w, open := <-writer:
			if !open {
				return
			}

			data, err := json.Marshal(w.Data)
			if err != nil {
				log.Fatal(err)
			}

			fmt.Fprintf(os.Stdout, "%s \n", data)
		}
	}
}

func getPage(ctx context.Context, query *ObjectQuery, r ReaderChannel, errs chan string) error {
	options := &options.FindOptions{}
	options.SetLimit(query.Limit)
	options.SetSkip(query.Page * query.Limit)

	cursor, err := query.Col.Find(ctx, query.Filter, options)
	if err != nil {
		log.Fatal(err)
	}

	for cursor.Next(ctx) {
		result := ObjectResult{}
		object := ObjectData{}
		if err = cursor.Decode(&object); err != nil {
			log.Fatal(err)
		}
		result.Data = object
		r <- result
	}

	return err
}
