
import boto3
import os
import requests
from botocore.exceptions import NoCredentialsError
from botocore.exceptions import ClientError
from tqdm import tqdm
import urllib3
import tkinter as tk
import base64
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import time
import concurrent.futures
import logging

app_id = 'bkv4gpres'
QB_TOKEN = 'QB-USER-TOKEN b5w6yk_v5c_0_ctwp43wb8p6hrzbjmn4h6bgrch5x'
# BV AWS credentials and region
bv_aws_access_key = 'AKIAXR5GICGZ6SD2QUXY'
bv_aws_secret_key = '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'
REGION = 'us-east-1'
BUCKET_NAME = "home-depot-garden-centers"  

logging.basicConfig(filename='error.log', level=logging.ERROR,  # Changed to DEBUG for more detailed logs
                    format='%(asctime)s:%(levelname)s:%(message)s')

s3_client = boto3.client('s3', region_name='us-east-1',
                         aws_access_key_id=bv_aws_access_key,
                         aws_secret_access_key=bv_aws_secret_key)

def timing_decorator(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"{func.__name__} took {elapsed_time:.4f} seconds to run.")
        return result
    return wrapper

def getTables(appID):
    headers = {
        'QB-Realm-Hostname': 'protrack.quickbase.com',
        'User-Agent': '{User-Agent}',
        'Authorization': QB_TOKEN
    }
    params = {
        'appId': appID
    }
    r = requests.get(
    'https://api.quickbase.com/v1/tables', 
    params = params, 
    headers = headers,
    verify=False
    )
    return r.json()

def is_base64_encoded(s):
    try:
        if isinstance(s, str):
            s_bytes = s.encode('utf-8')
        elif isinstance(s, bytes):
            s_bytes = s
        else:
            return False
        return base64.b64encode(base64.b64decode(s_bytes)) == s_bytes
    except Exception:
        return False
    
def create_s3_bucket(bucket_name, region):
    print(f"Creating bucket '{bucket_name}' in region '{region}'...")

    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' already exists.")
        return bucket_name
    except ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            try:
                s3_client.create_bucket(Bucket=bucket_name)
                print(f"Bucket '{bucket_name}' created successfully.")
                return bucket_name
            except ClientError as e:
                print(f"Error creating bucket '{bucket_name}': {e}")
                return False
        else:
            print(f"Error creating bucket '{bucket_name}': {e}")
            return False

def add_root_folder_to_s3(rootFolderName, bucketName, region='us-east-1' ):
    # S3 bucket name and folder name input
    bucket_name = bucketName
    folder_name = rootFolderName
    # Create an S3 client
    try:
        s3_client.put_object(Bucket=bucket_name, Key=f'{folder_name}/')
        
        print(f'Added folder "{folder_name}" to S3 bucket "{bucket_name}"')
        return folder_name
    except Exception as e:
        print(f'Error: {str(e)}')
    
def getfields(tableID, max_retries=5, initial_retry_delay=1, max_retry_delay=60):
    headers = {
        'QB-Realm-Hostname': 'protrack.quickbase.com',
        'User-Agent': '{User-Agent}',
        'Authorization': QB_TOKEN
    }
    params = {
        'tableId': {tableID}
    }

    retry_delay = initial_retry_delay
    num_retries = 0

    while num_retries < max_retries:
        try:
            r = requests.get(
                'https://api.quickbase.com/v1/fields',
                params=params,
                headers=headers,
                verify=False
            )
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            if r.status_code == 429:
                logging.error(f"Rate limit exceeded. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, max_retry_delay)  # Exponential backoff with a cap
                num_retries += 1
            else:
                logging.error(f"Error getting fields: {e}")
                return {'error': str(e)}
        except Exception as e:
            logging.error(f"Unexpected error getting fields: {e}")
            return {'error': str(e)}

    logging.error("Maximum number of retries reached. Unable to get fields.")
    return {'error': 'Maximum retries exceeded'}

def getRecords(tableID, select):
    headers = {
        'QB-Realm-Hostname': 'protrack.quickbase.com',
        'User-Agent': '{User-Agent}',
        'Authorization': QB_TOKEN
    }
    body = {
        "from": tableID,
        "select": select,
    }

    records = []
    request_payload = body.copy()
    while True:
        r = requests.post(
            'https://api.quickbase.com/v1/records/query',
            headers=headers,
            json=request_payload,
            verify=False
        )

        response_json = r.json()
        new_records = response_json.get('data', [])
        records.extend(new_records)
        
        if len(new_records) < 10000:
            break
        
        # Adjust the payload to get the next set of records
        request_payload = body.copy()
        request_payload["options"] = {
            "skip": len(records)
        }
    
    return records

def downloadFile(url):
    headers = {
        'QB-Realm-Hostname': 'protrack.quickbase.com',
        'User-Agent': '{User-Agent}',
        'Authorization': 'QB-USER-TOKEN b52bzj_v5c_0_cgxfs6ncz6nqgcdwwz6j9tcaw79'
    }
    path = 'https://api.quickbase.com/v1' + url
    r = requests.get(
        path,
        headers=headers,
        verify=False
    )
    return r.content

def upload_to_s3(bucket_name, object, rid, fieldName, fileName, bucketFolder, region):
    try:
        # Extract file content and name from the API response
        file_content = object
        # Upload the file to S3
        print(object)
        cleaned_attachment_name = fieldName.replace("/", "")
        s3_key = f"{bucketFolder}/{rid}/{cleaned_attachment_name}"
        s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=file_content)
        s3_object_url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket_name, 'Key': s3_key},
            ExpiresIn=31536000
        )
        print(f"{fileName} added to {s3_key}")
        return s3_object_url
    except NoCredentialsError:
        logging.error("Credentials not available.")
        return False
    except ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            logging.error(f"Error creating object '{s3_key}' in S3 bucket '{bucket_name}': Bucket does not exist.")
        else:
            logging.error(f"Error uploading object '{s3_key}' to S3 bucket '{bucket_name}': {e}")
        return False
    except requests.exceptions.RequestException as e:
        logging.error(f"Error making API request: {e}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error uploading object '{s3_key}' to S3 bucket '{bucket_name}': {e}")
        return False

def updateField(tableID, RID, FID, FidValue):
        headers = {
            'QB-Realm-Hostname': 'protrack.quickbase.com',
            'User-Agent': '{User-Agent}',
            'Authorization': QB_TOKEN
        }
        body = {
            "to": tableID ,
            "data": [
                    {
                        "3": {
                            "value": RID
                        },
                        FID:{
                            "value": FidValue
                        }

                    }
                ]
        }

        updateResponse = requests.post(
            'https://api.quickbase.com/v1/records', 
            headers = headers, 
            json = body,
            verify=False
        )
        return updateResponse.json()

def create_url_field(table_id, field_label):
    headers = {
        'QB-Realm-Hostname': 'protrack.quickbase.com',
        'User-Agent': '{User-Agent}',
        'Authorization': QB_TOKEN
    }
    params = {
        'tableId': table_id,
    }
    body = {
        'label': field_label,
        'fieldType': 'url'
    }
    r = requests.post(
        'https://api.quickbase.com/v1/fields', 
        params=params, 
        headers=headers, 
        verify=False,
        json=body
    )
    #print("Field Created: ", field_label, " in table ", table_id)
    return r.json()

def process_record(record, field_data, table_id, field_names, archive_fields, folder_name):
    files_processed = 0
    try:
        for field_id, field_info in field_data.items():
            if field_info['fieldType'] == 'file':
                attachments = record.get(str(field_id), [])
                if attachments:
                    original_field_name = field_names[field_id]
                    archive_field_name = f"{original_field_name}_ARCHIVE"

                    if archive_field_name not in archive_fields:
                        
                        new_field = create_url_field(table_id, archive_field_name)
                        #print(archive_fields  )
                        archive_fields[original_field_name] = new_field['id']
                        
                    

                    for attachment in attachments:
                        file_name = attachment['filename']
                        file_url = attachment['url']
                        print(file_url)

                        try:
                            response = requests.get(file_url, headers={'Authorization': QB_TOKEN}, verify=False)
                            response.raise_for_status()

                            s3_key = f"{folder_name}/{record['3']}/{original_field_name}/{file_name}"
                            s3_client.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=response.content)
                            s3_url = f"https://{BUCKET_NAME}.s3.amazonaws.com/{s3_key}"

                            update_result = update_record(table_id, record['3'], archive_fields[original_field_name], s3_url)

                            if update_result.get('metadata', {}).get('lineErrors', 0) == 0:
                                logging.info(f"Successfully processed and archived: {file_name}")
                                files_processed += 1
                            else:
                                logging.error(f"Failed to update QuickBase record for {file_name}: {update_result}")

                        except requests.RequestException as e:
                            logging.error(f"Failed to download file {file_name}: {str(e)}")
                        except ClientError as e:
                            logging.error(f"Failed to upload file {file_name} to S3: {str(e)}")
                        except Exception as e:
                            logging.error(f"Unexpected error processing {file_name}: {str(e)}")
    except Exception as e:
        logging.error(f"Unexpected error in process_record: {e}")

    return files_processed

def update_record(table_id, record_id, field_id, value):
    headers = {
        'QB-Realm-Hostname': 'protrack.quickbase.com',
        'User-Agent': '{User-Agent}',
        'Authorization': QB_TOKEN
    }
    body = {
        "to": table_id,
        "data": [
            {
                "rid": record_id,
                str(field_id): {
                    "value": value
                }
            }
        ]
    }
    logging.info(f"Updating QuickBase record: table_id={table_id}, record_id={record_id}, field_id={field_id}, value={value}")
    r = requests.post(
        'https://api.quickbase.com/v1/records',
        headers=headers,
        json=body,
        verify=False
    )
    logging.debug(f"Update response: {r.json()}")
    r.raise_for_status()
    return r.json()
# ---------------------------------------------------------------- #
# -----------------      MAIN FUNCTION    ------------------------ #
# ---------------------------------------------------------------- #
@timing_decorator
def archive_with_links():
    # Create the S3 bucket if it doesn't exist
    if not create_s3_bucket(BUCKET_NAME, REGION):
        return

    folders_added = []
    files_added = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count() * 2) as executor:
        for table in getTables(app_id):
            folder_name = table['name']
            table_id = table['id']
            folders_added.append(folder_name)

            field_data = {}
            field_names = {}
            archive_fields = {}

            fields = getfields(table_id)
            if not isinstance(fields, list):
                print(f"Unexpected output from getfields: {fields}")
                continue

            for field in fields:
                if isinstance(field, dict):
                    field_name = field.get('label', 'Unknown Label')
                    field_id = field.get('id', 'Unknown ID')
                    field_type = field.get('fieldType', 'Unknown Field Type')

                    field_data[field_id] = {'fieldType': field_type}
                    field_names[field_id] = field_name

                    if "_ARCHIVE" in field_name:
                        original_field_name = field_name.replace("_ARCHIVE", "")
                        archive_fields[original_field_name] = field_id
                else:
                    print(f"Unexpected field data type: {type(field)}")
                    print(f"Field content: {field}")
                    continue

            # Include the record ID field
            field_data['3'] = {'fieldType': 'recordid'}

            if any(info['fieldType'] == 'file' for info in field_data.values()):
                records = getRecords(table_id, list(field_data.keys()))

                futures = []
                for record in records:
                    futures.append(executor.submit(process_record, record, field_data, table_id, field_names, archive_fields, folder_name))

                for future in tqdm(concurrent.futures.as_completed(futures), total=len(records), desc=f"Processing {table['name']}"):
                    try:
                        files_processed = future.result()
                        files_added += files_processed
                    except Exception as e:
                        logging.error(f"Error in processing: {e}")

    print(f"Folders Added: {len(folders_added)}")
    print(f"Files Added: {files_added}")

archive_with_links()