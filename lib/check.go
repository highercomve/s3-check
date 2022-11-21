package lib

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/spf13/cobra"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
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

	fmt.Println(key)
	fmt.Println(secret)
	fmt.Println(region)
	fmt.Println(bucket)
	fmt.Println(endpoint)
	fmt.Println(connection)
	fmt.Println(database)
	fmt.Println(collection)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	storage, err := NewStorage(ctx, connection)
	if err != nil {
		return err
	}

	limit := int64(5)
	filter := bson.M{"sizeint": bson.M{"$gt": 0}}
	db := storage.GetDatabase(database)
	col := db.Collection(collection)

	count, err := col.CountDocuments(ctx, filter)
	if err != nil {
		return err
	}

	pages := count / limit

	err = getObjects(ctx, col, limit, pages, filter)
	if err != nil {
		return err
	}

	return nil
}

func getObjects(
	ctx context.Context,
	col *mongo.Collection,
	limit,
	pages int64,
	filter bson.M,
) (err error) {
	var wg sync.WaitGroup

	wg.Add(int(pages))

	for i := int64(0); i <= pages; i++ {
		err = getPage(ctx, col, filter, limit, i)
		if err != nil {
			return err
		}
	}

	return err
}

func getPage(
	ctx context.Context,
	col *mongo.Collection,
	filter bson.M,
	limit,
	page int64,
) (err error) {
	options := &options.FindOptions{}
	options.SetLimit(limit)
	options.SetSkip(page * limit)
	cursor, err := col.Find(ctx, filter, options)
	if err != nil {
		return err
	}

	for cursor.Next(ctx) {
		object := bson.M{}
		if err = cursor.Decode(&object); err != nil {
			return err
		}

		fmt.Printf("Page %d \n", page+1)
		fmt.Printf("%+v \n", object)
	}

	return err
}
