package lib

import (
	"context"

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
}

func NewS3Connect(ctx context.Context, params *S3ConnParams) (client *S3Client, err error) {
	credentials := credentials.NewStaticCredentialsProvider(params.Key, params.Secret, "")

	// Obtaining the S3 SDK client configuration based on the passed parameters.
	cnf, err := config.LoadDefaultConfig(
		ctx,
		config.WithCredentialsProvider(credentials),
		// Zerops supports only the S3 default region for API calls.
		// It doesn't mean that the physical HW infrastructure is located there also.
		// All Zerops infrastructure is completely located in Europe/Prague.
		config.WithRegion(params.Region),
	)
	if err != nil {
		return nil, err
	}

	c := s3.NewFromConfig(
		// Passing the S3 SDK client configuration created before.
		cnf,
		s3.WithEndpointResolver(
			// Applying of the Zerops Object Storage API URL endpoint.
			s3.EndpointResolverFromURL(params.Endpoint),
		),
		func(opts *s3.Options) {
			// Zerops supports currently only S3 path-style addressing model.
			// The virtual-hosted style model will be supported in near future.
			opts.UsePathStyle = true
		},
	)

	return &S3Client{client: c}, err
}
