S3 Checker
===================

Check object in a s3 storage

Usage:
  s3-check [flags]

```
Flags:
  -b, --bucket string       s3 BUCKET
  -c, --collection string   database collection
      --config string       config file (default is $HOME/.cobra.yaml)
  -m, --connection string   database connection url
      --cpuprofile string   CPU profiling
  -d, --database string     database name
  -e, --endpoint string     s3 ENDPOINT
  -h, --help                help for s3-check
  -k, --key string          s3 ACCESS_KEY
  -l, --limit int           Request limit (default 100)
  -a, --printall            Print all values in the database
  -r, --region string       s3 REGION
  -s, --secret string       s3 SECRET
```

You can run it loading a `.config.yaml` from $HOME/.config.yaml or the current path

you can load the configuration file from another path using the --config parameter.

The config file should be with this structure.

```yaml
key: S3_ACCESS_KEY_HERE
secret: S3_SECRET_HERE
region: S3_REGION_HERE
bucket: S3_BUCKET_HERE
database: MONGODB_DATABASE_NAME_HERE
collection: MONGODB_COLLECTION_NAME_HERE
connection: MONGODB_CONNECTION_URL
limit: 100
printall: false
```

## Run it using docker 

And you can run it using docker with the command.

```bash
docker run --rm -it -v ${PWD}/.config.yaml:/root/.config.yaml highercomve/s3-check:latest
```