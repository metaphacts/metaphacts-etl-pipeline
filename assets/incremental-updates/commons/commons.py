import boto3
import logging
import sys
import os
import gzip
import uuid

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
step_functions = boto3.client('stepfunctions')
s3c = boto3.client('s3')
s3r = boto3.resource('s3')

CONVERSION_BUCKET = os.environ.get('CONVERSION_BUCKET', '')
SOURCE_DATA_BUCKET = os.environ.get('SOURCE_DATA_BUCKET', '')
SOURCE_DATA_EXTENSION = '.jsonl.gz'
SOURCE_DATA_EXCEPTIONS = ['annotations/publications']
TARGET_DATA_BUCKET = os.environ.get('TARGET_DATA_BUCKET', '')
TARGET_DATA_EXTENSION = '.trig.gz'
INGESTED_MARK_FILE = 'ingested.txt.gz'
INGESTING_MARK_FILE = '.ingesting.txt.gz'
DELETE_SUFFIX = "_delete.txt.gz"
SPARQL_SUFFIX = ".sparql"
GRAPHDB_DOMAIN = os.environ.get('GRAPHDB_DOMAIN', 'http://localhost:7200/')
GRAPHDB_SPARQL = GRAPHDB_DOMAIN + 'repositories/metaphactory/statements'
GRAPHDB_LOGIN = GRAPHDB_DOMAIN + 'rest/login'
GRAPHDB_RUN_IMPORT = GRAPHDB_DOMAIN + 'rest/repositories/metaphactory/import/server'
GRAPHDB_CHECK_IMPORT = GRAPHDB_DOMAIN + 'rest/repositories/metaphactory/import/server'

GRAPHDB_USER = os.environ.get('GRAPHDB_USER', 'admin')
GRAPHDB_PASSWORD = os.environ.get('GRAPHDB_PASSWORD', 'root')

GRAPHDB_IMPORT_FOLDER_BASE = os.environ.get('GRAPHDB_IMPORT_FOLDER_BASE', '/data/rdf-files/additional-data/')
GRAPHDB_IMPORT_SUBFOLDER = os.environ.get('GRAPHDB_IMPORT_SUBFOLDER', 'deltas/')
GRAPHDB_IMPORT_FOLDER = os.environ.get('GRAPHDB_IMPORT_FOLDER', GRAPHDB_IMPORT_FOLDER_BASE+GRAPHDB_IMPORT_SUBFOLDER)


REDIS_SERVER = os.environ.get('REDIS_SERVER', 'localhost')
REDIS_PASSWORD = os.environ.get('REDIS_PASSWORD', 'admin')

RDF_BUCKET = os.environ.get('RDF_BUCKET', '')

POSTPROCESSING_FOLDER = os.environ.get('POSTPROCESSING_FOLDER', 'post-processing/')

#Other Utils

def divide_chunks_generator(l, n):
    idx = 0
    ls = []
    for item in l:
        ls.append(item)
        if len(ls)>n:
            response = ls
            ls = []
            idx = idx + 1
            yield idx, response
    if len(ls)>0:
        idx = idx + 1
        yield idx, ls

#S3 Utils

def list_files_bucket_suffix (bucket_name, prefix, suffix):
    for f in list_files_bucket(bucket_name, prefix):
        if f.endswith(suffix):
            yield f


def list_files_bucket_folders (bucket_name, prefix):
    result = s3c.list_objects(Bucket=bucket_name, Prefix=prefix, Delimiter='/')
    if result.get('CommonPrefixes') is not None:
        for o in result.get('CommonPrefixes'):
            folder = o.get('Prefix')
            yield folder
            for subfolder in list_files_bucket_folders(bucket_name, folder):
                yield subfolder


def list_files_bucket (bucket_name, prefix):
    paginator = s3c.get_paginator("list_objects_v2")
    response = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    for page in response:
        files = page.get("Contents")
        if files is None:
            return
        for file in files:
            yield file['Key']

def list_files_bucket_with_date (bucket_name, prefix):
    paginator = s3c.get_paginator("list_objects_v2")
    response = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    for page in response:
        files = page.get("Contents")
        if files is None:
            return
        for file in files:
            yield file['Key'],file['LastModified']

def list_folders_bucket_filter_unique (bucket_name, prefix, filter, exceptions):
    response_map = {}
    for result in list_files_bucket(bucket_name, prefix):
        skip = False
        for exception in exceptions:
            if result.startswith(exception):
                skip = True
                break
        if skip:
            continue
        if filter in result:
            folder = '/'.join(result.split('/')[:-1]) + '/'
            if folder not in response_map:
                response_map[folder]=None
                yield folder

def bucket_folder_contain_files (bucket_name, prefix, extension):
    for result in list_files_bucket(bucket_name, prefix):
        if result.endswith(extension):
            return True
    return False


def bucket_folder_contain_single_file (bucket_name, prefix):
    for result in list_files_bucket(bucket_name, prefix):
        return True
    return False

#Local files utils
def save_file_s3(bucket, prefix, data):
    fileName = f"/tmp/data{uuid.uuid4().hex}.sparql.gz"
    with gzip.open(fileName, 'wt') as f:
        f.write(data)
    s3r.meta.client.upload_file(fileName, bucket, prefix)
    os.remove(fileName)