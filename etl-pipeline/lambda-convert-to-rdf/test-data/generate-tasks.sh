#!/bin/sh

# this script generates task input for the conversion lambda 
# from a file listing produced using the following CLI command:
# aws s3 ls --recursive source-bucket >files2.txt

INPUT=$1

if [ -z "$INPUT" ]; then
  echo >&2 "call as $0 <inputfile>"
  exit 1
fi

echo >&2 "generating tasks from file $INPUT"

cat "$INPUT" | 
  grep -v artifact |  # get rid of artifact.json
  awk NF |           # get rid of empty lines
  awk '
  BEGIN { ORS = "" # do not print newline by default
    print "{\n \
    \"invocationSchemaVersion\": \"1.0\",\n \
    \"invocationId\": \"YXNkbGZqYWRmaiBhc2RmdW9hZHNmZGpmaGFzbGtkaGZza2RmaAo\",\n \
    \"job\": {\n \
        \"id\": \"f3cc4f60-61f6-4a2b-8a21-d07600c373ce\"\n \
    },\n \
    \"tasks\": [\n" 
  } 
  {
    if (FNR > 1) print ",\n"
    print "         {\n \
            \"taskId\": \"dGFza2lkZ29lc2hlcmUK-" FNR "\",\n \
            \"s3Key\": \""$0"\",\n \
            \"s3VersionId\": \"1\",\n \
            \"s3BucketArn\": \"arn:aws:s3:::source-bucket\"\n \
        }"
    

  }
  END { print "    ]\n}\n" } 
  '

exit 0

# this is how it should look
{
    "invocationSchemaVersion": "1.0",
    "invocationId": "YXNkbGZqYWRmaiBhc2RmdW9hZHNmZGpmaGFzbGtkaGZza2RmaAo",
    "job": {
        "id": "f3cc4f60-61f6-4a2b-8a21-d07600c373ce"
    },
    "tasks": [
        {
            "taskId": "dGFza2lkZ29lc2hlcmUK",
            "s3Key": "publications/0000001/records_000000001.jsonl.gz",
            "s3VersionId": "1",
            "s3BucketArn": "arn:aws:s3:::source-bucket"
        },
        {
            "taskId": "dGFza2lkZ29lc2hlcmUK-2",
            "s3Key": "publications/0000001/records_000013421.jsonl.gz",
            "s3VersionId": "1",
            "s3BucketArn": "arn:aws:s3:::source-bucket"
        }
    ]
}