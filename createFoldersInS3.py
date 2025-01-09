import boto3
import requests
from botocore.exceptions import NoCredentialsError, ClientError
import urllib3
import time
import concurrent.futures
import logging
from typing import List, Dict, Any

# Setup logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Constants
app_id = 'bkv4gpres'
QB_TOKEN = 'QB-USER-TOKEN b5w6yk_v5c_0_ctwp43wb8p6hrzbjmn4h6bgrch5x'
bv_aws_access_key = 'AKIAXR5GICGZ6SD2QUXY'
bv_aws_secret_key = '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'
REGION = 'us-east-1'
KEY_FIELD_ID = 3  # Define the key field ID as a constant


def timing_decorator(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        elapsed_time = end_time - start_time
        logging.info(f"{func.__name__} took {elapsed_time:.4f} seconds to run.")
        return result
    return wrapper

def getTables(appID):
    headers = {
        'QB-Realm-Hostname': 'protrack.quickbase.com',
        'Authorization': QB_TOKEN
    }
    params = {'appId': appID}
    r = requests.get('https://api.quickbase.com/v1/tables', params=params, headers=headers, verify=False)
    r.raise_for_status()
    logging.info("Fetched tables successfully.")
    return r.json()

def add_root_folder_to_s3(rootFolderName, bucketName):
    try:
        s3_client = boto3.client('s3', region_name=REGION,
                                 aws_access_key_id=bv_aws_access_key,
                                 aws_secret_access_key=bv_aws_secret_key)
        s3_client.put_object(Bucket=bucketName, Key=f'{rootFolderName}/')
        logging.info(f'Added folder "{rootFolderName}" to S3 bucket "{bucketName}"')
        return rootFolderName
    except ClientError as e:
        logging.error(f'ClientError adding folder to S3: {str(e)}')
        return False
    except Exception as e:
        logging.error(f'Error adding folder to S3: {str(e)}')
        return False

def getfields(tableID):
    headers = {
        'QB-Realm-Hostname': 'protrack.quickbase.com',
        'Authorization': QB_TOKEN
    }
    params = {'tableId': tableID}
    r = requests.get('https://api.quickbase.com/v1/fields', params=params, headers=headers, verify=False)
    r.raise_for_status()
    logging.info(f"Fetched fields for table {tableID}.")
    return r.json()

def getRecords(tableID, select=[]):
    headers = {
        'QB-Realm-Hostname': 'protrack.quickbase.com',
        'Authorization': QB_TOKEN
    }
    body = {"from": tableID, "select": select}
    
    try:
        r = requests.post('https://api.quickbase.com/v1/records/query', headers=headers, json=body, verify=False)
        r.raise_for_status()
        logging.info(f"Successfully fetched records for table {tableID}")
        return r.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching records from table {tableID}: {str(e)}")
        return {}

def downloadFile(url):
    headers = {
        'QB-Realm-Hostname': 'protrack.quickbase.com',
        'Authorization': QB_TOKEN
    }
    path = 'https://api.quickbase.com/v1' + url
    try:
        logging.info(f"Attempting to download file from {path}")
        r = requests.get(path, headers=headers, verify=False)
        r.raise_for_status()
        
        file_size = len(r.content)
        logging.info(f"Downloaded file from {url}, size: {file_size} bytes")
        
        if file_size == 0:
            logging.warning(f"Warning: No content downloaded from {url}.")
        return r.content
    except requests.exceptions.RequestException as e:
        logging.error(f"Error downloading file from {url}: {str(e)}")
        return None

def upload_to_s3(bucket_name, file_content, rid, fieldName, fileName, bucketFolder):
    if not file_content:
        logging.warning(f"No content to upload for {fileName}. Skipping upload.")
        return False

    s3_client = boto3.client('s3', region_name=REGION,
                             aws_access_key_id=bv_aws_access_key,
                             aws_secret_access_key=bv_aws_secret_key)
    try:
        cleaned_attachment_name = fieldName.replace("/", "")
        s3_key = f"{bucketFolder}/{rid}/{cleaned_attachment_name}/{fileName}"
        
        logging.info(f"Attempting to upload to {s3_key} in bucket {bucket_name}...")
        s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=file_content)
        logging.info(f"Successfully uploaded {fileName} to {s3_key}")
        return True
    except NoCredentialsError:
        logging.error("S3 Credentials not available.")
        return False
    except ClientError as e:
        logging.error(f"S3 Client error: {str(e)}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error uploading to S3: {str(e)}")
        return False

@timing_decorator
def process_record(record: Dict[str, Any], fieldDataForTable: List[int], tableID: str, 
                  fieldNames: Dict[int, str], folderName: str, bucket_name: str) -> int:
    files_added = 0
    try:
        rid = record.get(str(KEY_FIELD_ID), {}).get('value')
        if not rid:
            logging.warning(f"No record ID found for record in table {tableID}")
            return files_added

        for fileField in fieldDataForTable:
            try:
                field_value = record.get(str(fileField), {}).get("value", {})
                if not isinstance(field_value, dict):
                    continue
                
                url = field_value.get("url")
                if not url:
                    continue
                
                file_content = downloadFile(url)
                if not file_content:
                    continue
                
                versions = field_value.get("versions", [])
                if not versions:
                    continue
                
                fileName = versions[0].get("fileName")
                if not fileName:
                    logging.warning(f"No file name found for record {rid} in field {fileField}. Skipping.")
                    continue
                
                if upload_to_s3(bucket_name, file_content, rid, fieldNames[fileField], fileName, folderName):
                    files_added += 1
                    logging.info(f"Successfully uploaded file {fileName} for record {rid}")
            except Exception as e:
                logging.error(f"Error processing field {fileField} for record {rid}: {str(e)}")
    except Exception as e:
        logging.error(f"Error processing record in table {tableID}: {str(e)}")
    
    return files_added

@timing_decorator
def archiveWithLinks(newBucketName: str) -> int:
    total_files_added = 0
    rootFolderCreatedName = add_root_folder_to_s3(newBucketName, newBucketName)
    if not rootFolderCreatedName:
        logging.error("Failed to create root folder in S3.")
        return total_files_added

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_table = {}
        
        for table in getTables(app_id):
            try:
                folderName = add_root_folder_to_s3(table['name'], rootFolderCreatedName)
                tableID = table['id']

                fieldDataForTable = []
                fieldNames = {}

                for field in getfields(tableID):
                    field_id = field['id']
                    if field['fieldType'] == 'file' and field_id != KEY_FIELD_ID:
                        fieldDataForTable.append(field_id)
                        fieldNames[field_id] = field['label']

                if fieldDataForTable:
                    records = getRecords(tableID, fieldDataForTable + [KEY_FIELD_ID]).get("data", [])
                    logging.info(f"Processing {len(records)} records in table {table['name']} ({tableID})")
                    logging.info(f"File fields to process: {fieldDataForTable}")
                    
                    for record in records:
                        future = executor.submit(
                            process_record, 
                            record, 
                            fieldDataForTable, 
                            tableID, 
                            fieldNames, 
                            folderName,
                            rootFolderCreatedName
                        )
                        future_to_table[future] = table['name']
            except Exception as e:
                logging.error(f"Error setting up processing for table {table['name']}: {str(e)}")

        # Wait for all futures to complete and collect results
        for future in concurrent.futures.as_completed(future_to_table):
            table_name = future_to_table[future]
            try:
                files_added = future.result()
                total_files_added += files_added
            except Exception as e:
                logging.error(f"Error processing table {table_name}: {str(e)}")

    logging.info(f"Total Files Added: {total_files_added}")
    return total_files_added

if __name__ == "__main__":
    total_uploaded = archiveWithLinks("home-depot-garden-centers")
    logging.info(f"Script completed. Total files uploaded: {total_uploaded}")