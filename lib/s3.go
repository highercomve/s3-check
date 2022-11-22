package lib

import (
	"context"
	"strings"
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

	s3Client := &S3Client{bucket: params.Bucket}
	if params.Endpoint != "" {
		s3Client.client = s3.NewFromConfig(
			cnf,
			s3.WithEndpointResolver(
				s3.EndpointResolverFromURL(params.Endpoint),
			),
			func(opts *s3.Options) {
				opts.UsePathStyle = true
			},
		)
	} else {
		s3Client.client = s3.NewFromConfig(
			cnf,
			func(opts *s3.Options) {
				opts.UsePathStyle = true
			},
		)
	}

	return s3Client, err
}

func (s *S3Client) ObjectExist(ctx context.Context, id string) (exist bool, err error) {
	var obj *s3.HeadObjectOutput
	exist = false

	obj, err = s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &s.bucket,
		Key:    &id,
	})
	if err != nil && strings.Contains(err.Error(), "404") {
		return false, nil
	}
	if err != nil {
		return
	}

	if obj != nil {
		exist = true
	}

	return exist, err
}
