#!/bin/sh

# this script generates manifest file for the S3 Batch conversion 
# from a file listing produced using the following CLI command:
# aws s3 ls --recursive source-bucket >files2.txt

BUCKET=$1
INPUT=$2

if [ -z "$INPUT" ] || [ -z "$BUCKET" ]; then
  echo >&2 "call as $0 <bucket> <inputfile>"
  exit 1
fi

echo >&2 "generating tasks from file $INPUT"



cat "$INPUT" | 
  grep -v artifact |  # get rid of artifact.json
  awk NF |           # get rid of empty lines
  awk -v "bucket=$BUCKET" ' { print bucket "," $0 }'

exit 0

# this is how it should look
source-bucket, records_000000001.jsonl.gz
source-bucket, records_000000002.jsonl.gz
source-bucket, records_000000003.jsonl.gz
source-bucket, records_000000004.jsonl.gz
source-bucket, records_000013421.jsonl.gz