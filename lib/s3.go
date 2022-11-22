package lib

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3ConnParams struct {
	Key      string
	Secret   string
	Region   string
	Bucket   string
	Endpoint string
}

type S3Client struct {
	client *s3.Client
	bucket string
}

func NewS3Connect(ctxP context.Context, params *S3ConnParams) (client *S3Client, err error) {
	ctx, cancel := context.WithTimeout(ctxP, 10*time.Second)
	defer cancel()

	credentials := credentials.NewStaticCredentialsProvider(params.Key, params.Secret, "")

	cnf, err := config.LoadDefaultConfig(
		ctx,
		config.WithCredentialsProvider(credentials),
		config.WithRegion(params.Region),
	)
	if err != nil {
		return nil, err
	}

	c := s3.NewFromConfig(
		cnf,
		func(opts *s3.Options) {
			opts.UsePathStyle = true
		},
	)

	return &S3Client{client: c, bucket: params.Bucket}, err
}

func (s *S3Client) ObjectExist(ctx context.Context, id string) (exist bool, err error) {
	var obj *s3.HeadObjectOutput
	obj, err = s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &s.bucket,
		Key:    &id,
	})
	if err != nil {
		return
	}

	fmt.Printf("%+v", obj)

	if obj != nil {
		exist = false
	} else {
		exist = true
	}

	return exist, err
}
