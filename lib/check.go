package lib

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"runtime/pprof"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/time/rate"
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

type ObjectResult struct {
	Data map[string]interface{}
	Err  error
}

type Config struct {
	PrintAll  bool
	Stream    bool
	RateLimit int64
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
	stream := viper.GetBool("stream")
	ratelimit := viper.GetInt64("ratelimit")
	filterString := viper.GetString("filter")

	if cpuprofile != "" {
		f, err := os.Create(cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	config := Config{
		PrintAll:  printall,
		Stream:    stream,
		RateLimit: ratelimit,
	}
	parent := context.WithValue(context.Background(), ctxConfig, config)
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	storage, err := NewDbConnection(ctx, connection)
	if err != nil {
		return err
	}

	filter := map[string]interface{}{}
	if err := json.Unmarshal([]byte(filterString), &filter); err != nil {
		return err
	}
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
	channelSize := query.Count
	reader := make(ReaderChannel)
	writter := make(WriterChannel)
	result := make(ErrorChannel)
	quit := make(chan bool)

	go searchS3(ctx, query.S3Params, reader, writter, result)
	go writeResult(ctx, channelSize, writter, result, quit)

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

		if results >= int(channelSize) {
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
		go func(response ObjectResult) {
			defer func() {
				if r := recover(); r != nil {
					return
				}
			}()

			response.Data["exist"], err = s3.ObjectExist(ctx, response.Data["_id"].(string))
			if err != nil {
				errs <- err
				return
			}

			writter <- response
		}(resp)
	}
}

func writeResult(
	ctx context.Context,
	size int64,
	writer WriterChannel,
	results ErrorChannel,
	quit chan bool,
) {
	missing := 0
	total := 0
	config := ctx.Value(ctxConfig).(Config)

	spacer := ""
	endline := ""
	var output io.Writer
	if !config.Stream {
		output = new(bytes.Buffer)
	} else {
		spacer = "    "
		endline = "\n"
		output = os.Stdout
	}

	fmt.Fprintf(output, "{%s", endline)
	fmt.Fprintf(output, "%s\"elements\": [%s", spacer, endline)

writeLoop:
	for {
		select {
		case result := <-writer:
			total++
			exist, ok := result.Data["exist"]
			if !ok && !config.PrintAll {
				results <- nil
				continue writeLoop
			}
			if exist.(bool) && !config.PrintAll {
				results <- nil
				continue writeLoop
			}

			if !exist.(bool) {
				missing++
			}

			data, err := json.Marshal(result.Data)
			if err != nil {
				results <- err
				continue writeLoop
			}

			separator := ","
			if total >= int(size) {
				separator = ""
			}

			if _, err = fmt.Fprintf(output, "%s%s%s%s", spacer+spacer, data, separator, endline); err != nil {
				results <- err
				continue writeLoop
			}

			results <- nil
		case <-quit:
			break writeLoop
		}
	}

	fmt.Fprintf(output, "%s],%s", spacer, endline)
	fmt.Fprintf(output, "%s\"total\": %d,%s", spacer, total, endline)
	fmt.Fprintf(output, "%s\"missing\": %d%s", spacer, missing, endline)
	fmt.Fprintf(output, "}")

	if !config.Stream {
		_, err := os.Stdout.Write(output.(*bytes.Buffer).Bytes())
		if err != nil {
			log.Fatal(err)
		}
	}
	quit <- true
}

func getPage(ctx context.Context, query *ObjectQuery, r ReaderChannel) error {
	options := &options.FindOptions{}
	options.SetLimit(query.Limit)
	options.SetSkip(query.Page * query.Limit)

	config := ctx.Value(ctxConfig).(Config)
	var rateLimiter *rate.Limiter
	if config.RateLimit > 0 {
		rateLimiter = rate.NewLimiter(rate.Every(time.Second), int(config.RateLimit))
	}

	cursor, err := query.Col.Find(ctx, query.Filter, options)
	if err != nil {
		r <- ObjectResult{Err: err}
	}

	for cursor.Next(ctx) {
		result := ObjectResult{}
		object := map[string]interface{}{}
		if err = cursor.Decode(&object); err != nil {
			r <- ObjectResult{Err: err}
		}
		result.Data = object

		if rateLimiter != nil {
			if err := rateLimiter.Wait(ctx); err != nil {
				r <- ObjectResult{Err: err}
			}
		}

		r <- result
	}

	return err
}
