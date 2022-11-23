package lib

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"runtime/pprof"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
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
	key := viper.GetString("key")
	secret := viper.GetString("secret")
	region := viper.GetString("region")
	bucket := viper.GetString("bucket")
	endpoint := viper.GetString("endpoint")
	database := viper.GetString("database")
	collection := viper.GetString("collection")
	connection := viper.GetString("connection")
	printall := viper.GetBool("printall")
	limit := viper.GetInt64("limit")
	cpuprofile := viper.GetString("cpuprofile")

	if cpuprofile != "" {
		f, err := os.Create(cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
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
	reader := make(ReaderChannel)
	writter := make(WriterChannel)
	result := make(ErrorChannel)
	quit := make(chan bool)

	go searchS3(ctx, query.S3Params, reader, writter, result)
	go writeResult(ctx, writter, result, quit)

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
	reader ReaderChannel,
	writter WriterChannel,
	errs ErrorChannel,
) {
	s3, err := NewS3Connect(ctx, s3params)
	if err != nil {
		errs <- err
	}

	for resp := range reader {
		go func(r ObjectResult) {
			r.Data.Exist, err = s3.ObjectExist(ctx, r.Data.ID)
			if err != nil {
				errs <- err
				return
			}

			writter <- r
		}(resp)
	}
}

func writeResult(
	ctx context.Context,
	writer WriterChannel,
	errs ErrorChannel,
	quit chan bool,
) {
	missing := 0
	total := 0
	buf := new(bytes.Buffer)
	config := ctx.Value(ctxConfig).(Config)

	fmt.Fprintf(buf, "{\"elements\": [")

writeLoop:
	for {
		select {
		case result := <-writer:
			total++
			if result.Data.Exist && !config.PrintAll {
				errs <- nil
				continue
			}

			if !result.Data.Exist {
				missing++
			}

			data, err := json.Marshal(result.Data)
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

	_, err := os.Stdout.Write(buf.Bytes())
	if err != nil {
		log.Fatal(err)
	}
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
