import boto3
import base64
import os
import requests
import json
import urllib3
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import NoCredentialsError, ClientError
from tqdm import tqdm
import logging
import time

# Configure logging
logging.basicConfig(filename='error.log', level=logging.ERROR,
                    format='%(asctime)s:%(levelname)s:%(message)s')

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Define your constants
APP_ID = 'bi54diz7v'
BUCKET_NAME = "canes-protrack"
REGION = 'us-east-1'
QB_TOKEN = 'QB-USER-TOKEN b52bzj_v5c_0_hkfkdbdqdtgqubigsiu6dc9nq3f'
aws_access_key = 'AKIAXR5GICGZ6SD2QUXY'
aws_secret_key = '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'

# Create S3 client
s3_client = boto3.client('s3', region_name=REGION,
                         aws_access_key_id=aws_access_key,
                         aws_secret_access_key=aws_secret_key)

def timing_decorator(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"{func.__name__} took {elapsed_time:.4f} seconds to run.")
        return result
    return wrapper

def get_records(table_id, select=[]):
    headers = {
        'QB-Realm-Hostname': 'protrack.quickbase.com',
        'User-Agent': '{User-Agent}',
        'Authorization': QB_TOKEN
    }
    body = {
        "from": table_id,
        "select": select,
        "top" : 100
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
        
        request_payload = body.copy()
        request_payload["options"] = {
            "skip": len(records)
        }
    
    return records

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

def upload_to_s3(bucket_name, content, rid, field_name, file_name, bucket_folder):
    try:
        cleaned_attachment_name = field_name.replace("/", "")
        #s3_key = f"{bucket_folder}/{rid}/{cleaned_attachment_name}/{file_name}"
        s3_key = f"{bucket_folder}/{rid}/{file_name}"
        
        if is_base64_encoded(content):
            content = base64.b64decode(content)
        
        s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=content)

        s3_object_url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket_name, 'Key': s3_key},
            ExpiresIn=31536000 
        )
        logging.info(f"{file_name} added to {s3_key}")
        return s3_object_url

    except Exception as e:
        logging.error(f"Error uploading to S3: {e}")
        return False

def update_field(table_id, rid, fid, fid_value):
    headers = {
        'QB-Realm-Hostname': 'protrack.quickbase.com',
        'User-Agent': '{User-Agent}',
        'Authorization': QB_TOKEN
    }
    body = {
        "to": table_id,
        "data": [
            {
                "3": {"value": rid},
                fid: {"value": fid_value}
            }
        ]
    }

    response = requests.post(
        'https://api.quickbase.com/v1/records', 
        headers=headers, 
        json=body,
        verify=False
    )
    return response.json()

def get_tables(app_id):
    headers = {
        'QB-Realm-Hostname': 'protrack.quickbase.com',
        'User-Agent': '{User-Agent}',
        'Authorization': QB_TOKEN
    }
    params = {'appId': app_id}
    r = requests.get(
        'https://api.quickbase.com/v1/tables', 
        params=params, 
        headers=headers,
        verify=False
    )
    return r.json()

def get_fields(table_id):
    headers = {
        'QB-Realm-Hostname': 'protrack.quickbase.com',
        'User-Agent': '{User-Agent}',
        'Authorization': QB_TOKEN
    }
    params = {'tableId': table_id}
    r = requests.get(
        'https://api.quickbase.com/v1/fields', 
        params=params, 
        headers=headers,
        verify=False
    )
    return r.json()

def process_record(record, field_data, table_id, field_names, archive_fields, folder_name):
    rid = record["3"]['value']
    for file_field in field_data:
        if file_field == 3:
            continue
        try:
            url = record[str(file_field)]["value"]["url"]
            if url:
                fid = str(file_field)
                file_name = record[fid]["value"]["versions"][0]["fileName"]
                field_name = field_names[file_field]
                archive_fid = archive_fields[field_name]
                
                headers = {
                    'QB-Realm-Hostname': 'protrack.quickbase.com',
                    'User-Agent': '{User-Agent}',
                    'Authorization': QB_TOKEN
                }
                full_url = "https://api.quickbase.com/v1" + url
                response = requests.get(full_url, headers=headers, verify=False)
                response.raise_for_status()

                download_link = upload_to_s3(BUCKET_NAME, response.content, rid, field_name, file_name, folder_name)
                if download_link:
                    update_field(table_id, rid, archive_fid, download_link)
        except Exception as e:
            logging.error(f"Error processing record {rid}: {str(e)} | FID: {file_field}")

@timing_decorator
def archive_with_links():
    folders_added = []
    files_added = 0

    with ThreadPoolExecutor(max_workers=os.cpu_count() * 2) as executor:
        for table in get_tables(APP_ID):
            folder_name = table['name']
            table_id = table['id']
            folders_added.append(folder_name)

            field_data = []
            field_names = {}
            archive_fields = {}

            for field in get_fields(table_id):
                field_name = field['label']
                field_id = field['id']
                field_type = field['fieldType']

                if field_type == 'file':
                    field_data.append(field_id)
                    field_names[field_id] = field_name
                if "_ARCHIVE" in field_name:
                    original_field_name = field_name.replace("_ARCHIVE", "")
                    archive_fields[original_field_name] = field_id

            if field_data:
                field_data.append(3)
                records = get_records(table_id, field_data)

                futures = []
                for record in records:
                    futures.append(executor.submit(process_record, record, field_data, table_id, field_names, archive_fields, folder_name))

                for future in tqdm(as_completed(futures), total=len(records), desc=f"Processing {table['name']}"):
                    try:
                        future.result()
                        files_added += 1
                    except Exception as e:
                        logging.error(f"Error in processing: {e}")

    print(f"Folders Added: {len(folders_added)}")
    print(f"Files Added: {files_added}")

if __name__ == "__main__":
    archive_with_links()