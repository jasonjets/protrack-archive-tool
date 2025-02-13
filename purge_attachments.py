import requests
import urllib3
import logging
from typing import Dict, Any
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def purge_attachments(credentials: Dict[str, Any]) -> None:
    """
    Delete all file attachments from a QuickBase application.
    
    Args:
        credentials: Dictionary containing:
            - qb_realm_hostname: QuickBase realm hostname
            - qb_user_token: QuickBase user token
            - app_id: QuickBase application ID
    """
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    headers = {
        'QB-Realm-Hostname': f"{credentials['qb_realm_hostname']}.quickbase.com",
        'Authorization': f"QB-USER-TOKEN {credentials['qb_user_token']}",
        'Content-Type': 'application/json'
    }

    def get_tables():
        """Get all tables in the application."""
        params = {'appId': credentials['app_id']}
        r = requests.get('https://api.quickbase.com/v1/tables', 
                        params=params, headers=headers, verify=False)
        if not r.ok:
            raise Exception(f"Failed to get tables: {r.text}")
        return r.json()

    def get_fields(table_id):
        """Get all fields for a table."""
        r = requests.get(f'https://api.quickbase.com/v1/fields?tableId={table_id}', 
                        headers=headers, verify=False)
        if not r.ok:
            raise Exception(f"Failed to get fields: {r.text}")
        return r.json()

    def get_records_with_attachments(table_id, field_id):
        """Get records that have attachments in the specified field."""
        query_body = {
            "from": table_id,
            "select": [3, field_id],
            "where": "{'" + str(field_id) + "'.XEX.''}",
        }
        logger.info(f"Querying records for table ID {table_id}, field ID {field_id}")
        r = requests.post('https://api.quickbase.com/v1/records/query',
                        headers=headers, json=query_body, verify=False)
        if not r.ok:
            raise Exception(f"Failed to get records: {r.text}")
        
        return r.json().get('data', [])

    def delete_attachment(table_id, record_id, field_id):
        """Delete a file attachment."""
        # First get the file info to get the version number
        url = f'https://api.quickbase.com/v1/files/{table_id}/{record_id}/{field_id}/1'  # Try version 1
        r = requests.delete(url, headers=headers, verify=False)
        if not r.ok:
            raise Exception(f"Failed to delete attachment: {r.text}")

    try:
        tables = get_tables()
        logger.info(f"Found {len(tables)} tables in the application")

        # First pass to count total attachments
        total_attachments = 0
        for table in tables:
            fields = get_fields(table["id"])
            file_fields = [f for f in fields if f['fieldType'] == "file"]
            
            for field in file_fields:
                records = get_records_with_attachments(table["id"], field['id'])
                total_attachments += len(records)

        # Second pass to delete with progress bar
        deleted_attachments = 0
        with tqdm(total=total_attachments, desc="Deleting attachments") as pbar:
            for table in tables:
                fields = get_fields(table["id"])
                file_fields = [f for f in fields if f['fieldType'] == "file"]
                
                for field in file_fields:
                    records = get_records_with_attachments(table["id"], field['id'])
                    
                    for record in records:
                        record_id = record['3']['value']
                        try:
                            delete_attachment(table["id"], record_id, field['id'])
                            deleted_attachments += 1
                            pbar.update(1)
                        except Exception as e:
                            logger.error(f"Failed to delete attachment in table '{table['name']}', record {record_id}: {str(e)}")

        logger.info(f"\nPurge complete! Deleted {deleted_attachments} of {total_attachments} attachments")
                    
    except Exception as e:
        logger.error(f"Error during purge: {str(e)}")
        raise

if __name__ == "__main__":
    # Configuration
    credentials = {
        'qb_realm_hostname': 'protrack',  # Your realm hostname (without .quickbase.com)
        'qb_user_token': 'b5w6yk_v5c_0_ctwp43wb8p6hrzbjmn4h6bgrch5x',  # Your user token
        'app_id': 'bi54diz7v'  # Your app ID
    }

    # Run the purge
    purge_attachments(credentials) 