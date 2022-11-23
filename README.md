S3 Checker
===================

Check object in a s3 storage

Usage:
  s3-check [flags]

Flags:
  -b, --bucket string       s3 BUCKET
  -c, --collection string   database collection
      --config string       config file (default is $HOME/.cobra.yaml)
  -m, --connection string   database connection url
  -d, --database string     database name
  -e, --endpoint string     s3 ENDPOINT
  -h, --help                help for s3-check
  -k, --key string          s3 ACCESS_KEY
  -a, --printall            Print all values in the database
  -r, --region string       s3 REGION
  -s, --secret string       s3 SECRET