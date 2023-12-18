import sys
import json
import re
import secrets
import boto3
import logging
from datetime import datetime

step_functions = boto3.client('stepfunctions')
lambdac = boto3.client('lambda')
s3c = boto3.client('s3')
s3r = boto3.resource('s3')

def generate_and_save_manifest(ManifestPrefixScheduled):
    manifest_name = f"manifest-all.csv"
    uniq_file_name = secrets.token_hex(15)
    ManifestPrefix = f"{ManifestPrefixScheduled}{manifest_name}"
    ManifestLocalPath = f"/tmp/{uniq_file_name}-{manifest_name}"
    logger.info(f"Generating manifest")
    count_records = 0
    with open(ManifestLocalPath, "a+") as f:
        for file in list_files_bucket(sourceBucket, folder):
            if SOURCE_DATA_EXTENSION in file:
                count_records = count_records + 1
                line = f"{sourceBucket},{file}"
                f.write(f"{line}\n")
    return count_records, ManifestLocalPath, ManifestPrefix


def lambda_handler(event, context):
    runtimeBucket = event['runtimeBucket']
    sourceBucket = event['sourceBucket']
    sourcePrefix = event.get('sourcePrefix', '')
    sourcePattern = event.get('sourcePattern', '.*')

    logger.info(f"Generating manifest from bucket {sourceBucket}/{sourcePrefix} matching pattern {sourcePattern}")
        
    ManifestPrefixScheduled = f"manifests/ingestion-scheduled/"
    ManifestPrefixProcessed = f"manifests/ingestion-processed/"

    total_count = 0
    manifest_file_key = None

    manifest_name = f"manifest-all.csv"
    uniq_file_name = secrets.token_hex(15)
    manifest_file_key = f"{ManifestPrefixScheduled}{manifest_name}"
    manifest_local_path = f"/tmp/{uniq_file_name}-{manifest_name}"
    total_count = 0
    with open(manifest_local_path, "a+") as f:
        for file in list_files_bucket(sourceBucket, sourcePrefix, sourcePattern):
            total_count = total_count + 1
            line = f"{sourceBucket},{file}"
            f.write(f"{line}\n")
    
    logger.info(f"Uploading manifest for {total_count} files to {runtimeBucket}/{manifest_file_key}")
    s3r.meta.client.upload_file(manifest_local_path,runtimeBucket,manifest_file_key)
    s3_resp = s3c.head_object(Bucket=runtimeBucket, Key=manifest_file_key)
    etag = s3_resp['ETag'].strip('"')

    logger.info(f"Created S3 Batch Operations manifest with {total_count} files to convert to RDF.")
    result  = { 
        "Payload" : { 
            "ManifestBucket" : runtimeBucket, 
            "ManifestKey" : manifest_file_key, 
            "ManifestEtag" : etag, 
            "Scheduled" : ManifestPrefixScheduled,
            "ProcessedKey" : manifest_file_key.replace(ManifestPrefixScheduled, ManifestPrefixProcessed), 
            "TotalFileCount" : total_count, 
        }
    }
    logger.info(result)

    task_token = event['taskToken']
    if task_token:
        logger.info(f"Notify Step Functions task to continue.")
        step_functions.send_task_success(
            taskToken=task_token,
            output=json.dumps(result)
        )

def list_files_bucket(bucket_name, prefix, pattern):
    paginator = s3c.get_paginator("list_objects_v2")
    response = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    compiledPattern = None
    if pattern is not None:
        compiledPattern = re.compile(pattern)
    
    for page in response:
        files = page.get("Contents")
        if files is None:
            return
        for file in files:
            fileName = file['Key']
            if compiledPattern is not None:
                if not compiledPattern.match(fileName):
                    # ignore this file
                    logger.info(f"skipping file {fileName}")
                    continue
            yield fileName


LOGGING_LEVEL = logging.INFO
logger = logging.getLogger(__name__)
logger.setLevel(LOGGING_LEVEL)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(LOGGING_LEVEL)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
if logger.hasHandlers():
    logger.handlers.clear()
logger.addHandler(handler)

# test handler
#testRuntimeBucket = 'etlpipelinestack-covid-runtimebucket77ae14c3-10mgknkkmpw0'
#testSourceBucket = 'etlpipelinestack-covid-sourcebucket5c83c9d6-vavdiprznm3e'
#testTaskToken = '' # 'XXX-token-XXX'
#lambda_handler({'taskToken':testTaskToken, 'runtimeBucket':testRuntimeBucket, 'sourceBucket':testSourceBucket, 'sourcePrefixXXX':'', 'sourcePattern':'.*\\.csv'}, None)
