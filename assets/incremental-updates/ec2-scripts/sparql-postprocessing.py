from commons.commons import *
from commons.graphdb import *
import json
import uuid
import os

def main():

    logger.info("Adding DCAT Metadata Last Modified.")
    last_versions = {}
    for source_folder in list_files_bucket_folders(RDF_BUCKET, ""):
        source_folder_parts = source_folder.split('/')
        if len (source_folder_parts) != 5:
            continue
        dataset = source_folder_parts[0]
        type = source_folder_parts[1]
        release = "/".join(source_folder_parts)
        last_update = None
        for _, last_update_source in list_files_bucket_with_date(SOURCE_DATA_BUCKET, release):
            last_update = last_update_source.strftime("%Y-%m-%dT%H:%M:%SZ")
        dcat_name = type

        # TODO adjust dcat_name based on types?

        # TODO make IRI configurable
        iri = f"https://example.com/dcat/{dcat_name}"
        if iri in last_versions:
            if last_update < last_versions[iri]['last_update']:
                last_update = last_versions[iri]['last_update']
        last_versions[iri] = {"iri":iri, "dataset": dataset, "type":type, "release":release, "last_update":last_update}
    update_dcat_last_modified([ value for value in last_versions.values()])
    logger.info("Adding DCAT Metadata Last Modified... Finished.")
    results = []
    logger.info(f"Start post processing butcket:{CONVERSION_BUCKET} folder:{POSTPROCESSING_FOLDER}")
    for sparql in list_files_bucket_suffix(CONVERSION_BUCKET, POSTPROCESSING_FOLDER, SPARQL_SUFFIX):
        tmpFile = f'/tmp/{uuid.uuid4().hex}.sparql'
        s3c.download_file(CONVERSION_BUCKET, sparql, tmpFile)
        query =''
        status = ''
        with open(tmpFile, 'r') as file:
            query = file.read().rstrip()
        try:
            logger.info(f"Start execution of query : {sparql}")
            logger.info(query)
            #update(query)
            status = "OK"
            logger.info(f"Execution of query : {sparql} was sucessful")
        except Exception as e:
            logger.info(f"Execution of query : {sparql} finished with  error {e}")
            status = f"ERROR, {e}"
        result = {"query":sparql, 'status':status}
        results.append(result)
        os.remove(tmpFile)
    logger.info(results)
    logger.info("Post processing finished.")
main()