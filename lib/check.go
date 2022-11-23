package lib

import (
	"bytes"
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

type contextConfig string

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
	ID    string `json:"id" bson:"_id"`
	Exist bool   `json:"exist" bson:"exist"`
}

type ObjectResult struct {
	Data ObjectData
	Err  error
}

type Config struct {
	PrintAll bool
}

type ReaderChannel chan ObjectResult
type WriterChannel chan ObjectResult
type ErrorChannel chan error

const (
	ctxConfig = contextConfig("config")
)

func CheckStorage(cmd *cobra.Command, args []string) (err error) {
	var key string
	var secret string
	var region string
	var bucket string
	var endpoint string
	var connection string
	var database string
	var collection string
	var printall bool

	if key, err = cmd.Flags().GetString("key"); err != nil {
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
	if printall, err = cmd.Flags().GetBool("printall"); err != nil {
		return err
	}

	parent := context.WithValue(
		context.Background(),
		ctxConfig,
		Config{PrintAll: printall})
	ctx, cancel := context.WithCancel(parent)
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
	channelSize := query.Count - 1
	reader := make(ReaderChannel, channelSize)
	writter := make(WriterChannel, channelSize)
	result := make(ErrorChannel, channelSize)
	quit := make(chan bool)

	go func() {
		searchS3(ctx, query.S3Params, reader, writter, result)
	}()
	go func() {
		writeResult(ctx, writter, result, quit)
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
			err = getPage(ctx, &query, reader)
			if err != nil {
				log.Fatal(err)
			}
		}(ctx)
	}

	results := 0
	for e := range result {
		results++
		if e != nil {
			err = e
			break
		}

		if results > int(channelSize) {
			break
		}
	}

	quit <- false
	<-quit

	close(reader)
	close(writter)
	close(result)
	close(quit)

	return err
}

func searchS3(
	ctx context.Context,
	s3params *S3ConnParams,
	s ReaderChannel,
	w WriterChannel,
	errs ErrorChannel,
) {
	s3, err := NewS3Connect(ctx, s3params)
	if err != nil {
		errs <- err
	}

	for r := range s {
		r.Data.Exist, err = s3.ObjectExist(ctx, r.Data.ID)
		if err != nil {
			errs <- err
			break
		}

		w <- r
	}
}

func writeResult(ctx context.Context, writer WriterChannel, errs ErrorChannel, quit chan bool) {
	missing := 0
	total := 0
	buf := new(bytes.Buffer)
	config := ctx.Value(ctxConfig).(Config)

	fmt.Fprintf(buf, "{")
	fmt.Fprintf(buf, "\"elements\": [")

writeLoop:
	for {
		select {
		case w := <-writer:
			total++
			if w.Data.Exist && !config.PrintAll {
				errs <- nil
				continue
			}

			if !w.Data.Exist {
				missing++
			}

			data, err := json.Marshal(w.Data)
			if err != nil {
				errs <- err
				continue
			}

			fmt.Fprintf(buf, "%s,", data)
			errs <- nil
		case <-quit:
			break writeLoop
		}
	}

	buf.Truncate(len(buf.Bytes()) - 1)
	fmt.Fprintf(buf, "],")
	fmt.Fprintf(buf, "\"total\": %d,", total)
	fmt.Fprintf(buf, "\"missing\": %d", missing)
	fmt.Fprintf(buf, "}")

	os.Stdout.Write(buf.Bytes())
	quit <- true
}

func getPage(ctx context.Context, query *ObjectQuery, r ReaderChannel) error {
	options := &options.FindOptions{}
	options.SetLimit(query.Limit)
	options.SetSkip(query.Page * query.Limit)

	cursor, err := query.Col.Find(ctx, query.Filter, options)
	if err != nil {
		r <- ObjectResult{Err: err}
	}

	for cursor.Next(ctx) {
		result := ObjectResult{}
		object := ObjectData{}
		if err = cursor.Decode(&object); err != nil {
			r <- ObjectResult{Err: err}
		}
		result.Data = object
		r <- result
	}

	return err
}
