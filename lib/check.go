package lib

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type ObjectsQuery struct {
	Ctx    context.Context
	Col    *mongo.Collection
	Limit  int64
	Filter bson.M
	Pages  int64
}

type ObjectQuery struct {
	*ObjectsQuery
	Page int64
}

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

	err = getObjects(&ObjectsQuery{
		Ctx:   ctx,
		Pages: pages,
		Limit: limit,
		Col:   col,
	})
	if err != nil {
		return err
	}

	return nil
}

func getObjects(query *ObjectsQuery) (err error) {
	for i := int64(0); i <= query.Pages; i++ {
		page := i
		query := &ObjectQuery{
			ObjectsQuery: query,
			Page:         page,
		}
		if err = getPage(query); err != nil {
			return err
		}
	}

	return err
}

func getPage(query *ObjectQuery) (err error) {
	options := &options.FindOptions{}
	options.SetLimit(query.Limit)
	options.SetSkip(query.Page * query.Limit)

	cursor, err := query.Col.Find(query.Ctx, query.Filter, options)
	if err != nil {
		return err
	}

	for cursor.Next(query.Ctx) {
		object := bson.M{}
		if err = cursor.Decode(&object); err != nil {
			return err
		}

		fmt.Printf("Page %d \n", query.Page+1)
		fmt.Printf("%+v \n", object)
	}

	return err
}
