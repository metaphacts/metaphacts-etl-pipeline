import json
import redis
from commons.commons import *
from datetime import datetime
import secrets
lambdac = boto3.client('lambda')

def get_last_basesets(skipMetrics):
    last_basesets = {}    
    for source_folder in list_files_bucket_folders(SOURCE_DATA_BUCKET, ''):
        source_folder_parts = source_folder.split('/')
        if len (source_folder_parts) < 5:
            continue
        logger.info(f"Checking folders {source_folder}")
        dataset = source_folder_parts[0]
        type = source_folder_parts[1]
        release = '/'.join(source_folder_parts[0:3]) + '/'
        ##Especial case for metrics
        if type == 'metrics':
            if skipMetrics:
                continue
            release = '/'.join(source_folder_parts[0:4]) + '/'
        key = dataset + type
        last_basesets [key] = { 'dataset':dataset, 'type':type, 'last_release':release}
    return last_basesets

def generate_and_save_manifest(batch, folders, ManifestPrefixScheduled, uniq_file_name):
    manifest_name = f"manifest-all.csv"
    ManifestPrefix = f"{ManifestPrefixScheduled}{manifest_name}"
    ManifestLocalPath = f"/tmp/{uniq_file_name}-{manifest_name}"
    logger.info(f"Generating manifests batch : {batch}")
    count_records = 0
    with open(ManifestLocalPath, "a+") as f:
        for idx, folder in enumerate(folders):
            logger.debug(f"Batch {batch} contains folder {folder}")
            for file in list_files_bucket(SOURCE_DATA_BUCKET, folder):
                if SOURCE_DATA_EXTENSION in file:
                    count_records = count_records + 1
                    line = f"{SOURCE_DATA_BUCKET},{file}"
                    f.write(f"{line}\n")
    return count_records, ManifestLocalPath, ManifestPrefix

def init_conversion():
    logger.info(f"Cleaning up redis {REDIS_SERVER} .")
    con = redis.Redis(host=REDIS_SERVER, port=6379, password=REDIS_PASSWORD)
    con.flushall(True)
    logger.info(f"Cleaning up redis finished.")
    version = datetime.today().strftime('%Y%m%d')
    new_s3_bucket = f"output-rdf-export-etl-{version}"
    #Create bucket
    s3c.create_bucket(Bucket=new_s3_bucket)
    return new_s3_bucket, version


def config_conversion_function(new_s3_bucket, coldStart):
    lambdac.update_function_configuration(
                FunctionName='JsonToRdf',
                Environment={
                    'Variables': {
                        'UPLOAD_BUCKET': new_s3_bucket,
                        'REDIS_SERVER': REDIS_SERVER,
                        'REDIS_PASSWORD': REDIS_PASSWORD,
                        "PROCESS_DETECT_LASTUPDATE":"true" if coldStart else "false",
                        "PROCESS_COLDSTART":"true" if coldStart else "false" 
                    }
                }
            )

def lambda_handler(event, context):
    warnings = ""
    isColdStart = True
    skipMetrics = False
    newBucket = ''
    if event['input']['mode'] == 'cold-start':
        newBucket, version = init_conversion()
        if 'rdf-bucket' in event['input']:
            logger.info(f"Overriding bucket {newBucket} -> {event['input']['rdf-bucket']}")
            newBucket = event['input']['rdf-bucket']
        logger.info(f"Initializing cold start... Version:{version} Bucket:{newBucket}")
    elif event['input']['mode'] == 'incremental-updates':
        if 'skipMetrics' in event['input'] and event['input']['skipMetrics'] == True:
            skipMetrics = True
        newBucket = event['input']['rdf-bucket']
        version = datetime.today().strftime('%Y%m%d')
        logger.info(f"Initializing incremental updates... Version:{version} Bucket:{newBucket}")
        isColdStart = False
    else:
        raise Exception("Invalid operation mode") 
    config_conversion_function(newBucket, isColdStart )
    ManifestPrefixScheduled = f"manifests/ingestion-{version}-scheduled/"
    ManifestPrefixProcessed = f"manifests/ingestion-{version}-processed/"
    logger.info(f"Collecting last basesets")
    basesets = get_last_basesets(skipMetrics)
    batches_count = 0
    for key in basesets:
        baseset = basesets[key]
        logger.info(f"Collecting all folders of dataset: {baseset['dataset']} type: {baseset['type']} release: {baseset['last_release']}")
        baseset ['folders'] = []
        folders_raw_list = [f for f in list_files_bucket_folders(SOURCE_DATA_BUCKET, baseset['last_release'])]
        ##Especial case, Metrics data do not have subfolders
        if not folders_raw_list:
            folders_raw_list = [baseset['last_release']]
        for folder in folders_raw_list:
            if bucket_folder_contain_files(newBucket, folder, TARGET_DATA_EXTENSION):
                logger.info(f"Folder : {folder} was already converted to RDF, skipping...")
                continue
            if not isColdStart:
                if "/00000001/" in folder:
                    warnings = warnings + f"{folder} is a baseset not an incremental update, a new cold start ingestion should be executed."
                    logger.info(f"Folder : {folder} is a baseset not an incremental update, skipping...")
                    continue
            baseset['folders'].append(folder)
            folders_count = len(baseset['folders'])
            batches_count = folders_count if folders_count > batches_count else batches_count
    logger.info(f"Generating batches / manifests")
    total_count = 0
    manifest_file_key = None
    manifest_local_path = None
    uniq_file_name = secrets.token_hex(15)
    for batch in range(batches_count):
        folder_list = []
        logger.info(f"Generating batch {batch} / {batches_count}")
        for key in basesets:
            baseset = basesets[key]
            if len(baseset ['folders']) > 0:
                folder = baseset['folders'].pop(0)
                folder_list.append(folder)
                logger.info(f"Batch {batch} includes dataset:{baseset['dataset']} type:{baseset['type']} folder: {folder}")
        count_records, manifest_local, manifest_file = generate_and_save_manifest(batch, folder_list, ManifestPrefixScheduled, uniq_file_name)
        manifest_file_key = manifest_file
        manifest_local_path = manifest_local
        total_count = total_count + count_records
        logger.info(f"Generating manifest adding {count_records} files")
    s3r.meta.client.upload_file(manifest_local_path,CONVERSION_BUCKET,manifest_file_key)
    s3_resp = s3c.head_object(Bucket=CONVERSION_BUCKET, Key=manifest_file_key)
    etag = s3_resp['ETag'].strip('"')

    logger.info(f"There are {total_count} files that will converted.")
    result  = {"Payload":{"ManifestBucket":CONVERSION_BUCKET, "ManifestKey":manifest_file_key, "ManifestEtag":etag, "OutputBucket":newBucket, "Scheduled":ManifestPrefixScheduled ,"ProcessedKey":manifest_file_key.replace(ManifestPrefixScheduled, ManifestPrefixProcessed), "Version":version, "BatchesCount":batches_count, "TotalFileCount":total_count, "Mode":event['input']['mode'], "Warning":warnings}}
    logger.info(result)
    task_token = event['taskToken']
    step_functions.send_task_success(
        taskToken=task_token,
        output=json.dumps(result)
    )
#lambda_handler({'input':{'mode':'cold-start'}}, None)
#lambda_handler({'input':{'mode':'incremental-updates', 'rdf-bucket':'output-rdf-export-etl-20230907'}}, None)
