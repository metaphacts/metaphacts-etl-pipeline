import boto3
import rdflib
from multiprocessing import Manager
from concurrent.futures import ProcessPoolExecutor
from rdflib import URIRef
import os


from multiprocessing import Manager
from concurrent.futures import ProcessPoolExecutor
import logging
import hashlib
import itertools
from commons.graphdb import *
from commons.commons import *

fh = logging.FileHandler('ingestion.log')
fh.setFormatter(formatter)
logger.addHandler(fh)

DELETE_BATCH_SIZE = 10000
NTHREADS = 2

def read_delete_iris(path):
    with gzip.open(path) as infile:
        for line in infile:
            yield line.rstrip().decode("utf-8") 

def ingest(bucket, prefix_delete):
    prefix_insert = prefix_delete.replace(DELETE_SUFFIX,"")

    prefix_insert_parts = prefix_insert.split('/')
    dataset = prefix_insert_parts[0]
    type = prefix_insert_parts[1]

    restrict_delete = None

    logger.info(f"Start ingestion of file {prefix_insert}")
    uid = uuid.uuid4().hex

    #Create mark file to indicate that ingestion is running
    save_file_s3(RDF_BUCKET, prefix_insert + INGESTING_MARK_FILE, "")
    
    #Delete
    data_path= f'/tmp/delete_{uid}.txt.gz'
    s3c.download_file(bucket, prefix_delete, data_path)
    for idx, irisToDeleteBatch in divide_chunks_generator(read_delete_iris(data_path), DELETE_BATCH_SIZE):
        q = build_delete_query(irisToDeleteBatch, restrict_delete)
        logger.info(f"Running deletes for file {prefix_insert}")
        update(q)
    os.remove(data_path)
    #Insert
    filename_original = prefix_insert.split('/')[-1]
    insert_path = GRAPHDB_IMPORT_FOLDER + uid + filename_original
    filename = GRAPHDB_IMPORT_SUBFOLDER + insert_path.split('/')[-1]
    s3c.download_file(bucket, prefix_insert, insert_path)
    logger.info(f"Running insert for file {prefix_insert} , local file : {filename}")
    import_and_wait(filename)
    os.remove(insert_path)

    #Delete mark file
    s3r.Object(RDF_BUCKET, prefix_insert + INGESTING_MARK_FILE).delete()
    logger.info(f"End ingestion of file {prefix_insert}")


def main():

    if not os.path.isdir(GRAPHDB_IMPORT_FOLDER):
        os.makedirs(GRAPHDB_IMPORT_FOLDER)

    logger.info("Checking folders to ingest.")
    for rdf_folder in list_folders_bucket_filter_unique(RDF_BUCKET, '', TARGET_DATA_EXTENSION, []):
        if bucket_folder_contain_single_file(RDF_BUCKET, rdf_folder + INGESTED_MARK_FILE):
            logger.info(f"Folder {rdf_folder} was already ingested.")
        else:
            logger.info(f"Folder {rdf_folder} will be ingested, using {NTHREADS} threads.")
            with Manager() as manager:
                executor = ProcessPoolExecutor(NTHREADS)
                futures = [executor.submit(ingest,RDF_BUCKET,_) for _ in list_files_bucket_suffix(RDF_BUCKET, rdf_folder, DELETE_SUFFIX)]
                for future in futures:
                    future.result()
            logger.info(f"Ingestion finished, adding mark file to folder {rdf_folder}.")
            save_file_s3(RDF_BUCKET,rdf_folder + INGESTED_MARK_FILE, "")

main()
