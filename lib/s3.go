package lib

import (
	"context"
	"log"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var endpoint string = "s3.amazonaws.com"

type S3ConnParams struct {
	Key      string
	Secret   string
	Region   string
	Bucket   string
	Endpoint string
}

type S3Client struct {
	client *minio.Client
	bucket string
}

func NewS3Connect(ctxP context.Context, params *S3ConnParams) (client *S3Client, err error) {
	s3Client := &S3Client{bucket: params.Bucket}
	if params.Endpoint != "" {
		endpoint = params.Endpoint
	}

	s3Client.client, err = minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(params.Key, params.Secret, ""),
		Region: params.Region,
		Secure: true,
	})
	if err != nil {
		log.Fatalln(err)
	}

	return s3Client, err
}

func (s *S3Client) ObjectExist(ctx context.Context, id string) (exist bool, err error) {
	exist = false

	_, err = s.client.StatObject(ctx, s.bucket, id, minio.StatObjectOptions{})
	if err != nil && strings.Contains(err.Error(), "The specified key does not exist") {
		return false, nil
	}
	if err != nil {
		return
	}

	exist = true

	return exist, err
}
