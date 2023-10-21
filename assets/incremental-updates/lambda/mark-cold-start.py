from commons.commons import *
import json

def lambda_handler(event, context):
    bucket = event['input']['OutputBucket']
    logger.info("Cold start ingestion finished.")
    logger.info(f"Add mark files in all ingested folders in the bucket {bucket}.")
    count =0
    for source_folder in list_files_bucket_folders(bucket, ''):
        source_folder_parts = [f for f in source_folder.split('/') if f!='']
        #Mark only leave folders
        if len (source_folder_parts) == 4 :
            logger.info(f"Marking folder {source_folder} as ingested")
            count = count+1
            save_file_s3(bucket,source_folder + INGESTED_MARK_FILE, "")
    result = {"Payload":{"MarkedBucket":bucket, "FolderCount":count}}
    logger.info(result)
    task_token = event['taskToken']
    step_functions.send_task_success(
        taskToken=task_token,
        output=json.dumps(result)
    )

#lambda_handler({'input':{}},{})