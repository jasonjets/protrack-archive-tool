import tkinter as tk
from tkinter import ttk, scrolledtext
import requests
import urllib3
import json
import boto3
import concurrent.futures
import logging
from typing import Dict, Any

class QuickbaseArchiveApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Quickbase Archive Tool")
        self.geometry("600x500")
        self.configure(bg="#f0f0f0")

        # Configure styling
        self.style = ttk.Style(self)
        self.style.configure('TLabel', background="#f0f0f0", font=('Helvetica', 10))
        self.style.configure('TEntry', font=('Helvetica', 10))
        self.style.configure('TButton', font=('Helvetica', 10), padding=5)
        self.style.configure('TNotebook.Tab', font=('Helvetica', 10))

        # Create notebook (tabbed interface)
        self.notebook = ttk.Notebook(self)
        self.notebook.pack(expand=True, fill="both", padx=10, pady=10)

        # Create tabs
        self.upload_tab = ttk.Frame(self.notebook)
        self.url_fields_tab = ttk.Frame(self.notebook)
        
        self.notebook.add(self.upload_tab, text="Upload Files")
        self.notebook.add(self.url_fields_tab, text="Create URL Fields")

        # Initialize tabs
        self.create_upload_tab()
        self.create_url_fields_tab()

        # Create shared credentials frame
        self.create_shared_credentials_frame()

    def create_shared_credentials_frame(self):
        cred_frame = ttk.LabelFrame(self, text="Shared Credentials", padding=10)
        cred_frame.pack(fill="x", padx=10, pady=5)

        self.shared_entries = {}
        shared_fields = [
            ("AWS Access Key:", "aws_access_key"),
            ("AWS Secret Key:", "aws_secret_key", True),
            ("QB Realm Hostname:", "qb_realm_hostname"),
            ("QB User Token:", "qb_user_token", True),
            ("QB App ID:", "app_id")
        ]

        for i, field_info in enumerate(shared_fields):
            label_text = field_info[0]
            field_key = field_info[1]
            is_secret = len(field_info) > 2 and field_info[2]

            label = ttk.Label(cred_frame, text=label_text)
            label.grid(row=i, column=0, sticky="e", padx=5, pady=2)
            
            entry = ttk.Entry(cred_frame, show="*" if is_secret else "")
            entry.grid(row=i, column=1, sticky="ew", padx=5, pady=2)
            self.shared_entries[field_key] = entry

        cred_frame.columnconfigure(1, weight=1)

    def create_upload_tab(self):
        frame = ttk.Frame(self.upload_tab, padding="10")
        frame.pack(fill=tk.BOTH, expand=True)

        # S3 bucket input
        ttk.Label(frame, text="S3 Bucket Name:").pack(anchor='w', pady=2)
        self.bucket_entry = ttk.Entry(frame)
        self.bucket_entry.pack(fill=tk.X, pady=2)

        # Upload button
        self.upload_button = ttk.Button(frame, text="Start Upload", command=self.run_upload)
        self.upload_button.pack(pady=10)

        # Log output
        ttk.Label(frame, text="Log Output:").pack(anchor='w', pady=2)
        self.log_output = scrolledtext.ScrolledText(frame, height=10)
        self.log_output.pack(fill=tk.BOTH, expand=True)

    def create_url_fields_tab(self):
        frame = ttk.Frame(self.url_fields_tab, padding="10")
        frame.pack(fill=tk.BOTH, expand=True)

        # Create URL fields button
        self.create_fields_button = ttk.Button(frame, text="Create Archive URL Fields", 
                                              command=self.run_create_url_fields)
        self.create_fields_button.pack(pady=10)

        # Status output
        ttk.Label(frame, text="Status:").pack(anchor='w', pady=2)
        self.url_status_output = scrolledtext.ScrolledText(frame, height=10)
        self.url_status_output.pack(fill=tk.BOTH, expand=True)

    def get_credentials(self) -> Dict[str, str]:
        return {key: entry.get() for key, entry in self.shared_entries.items()}

    def run_upload(self):
        creds = self.get_credentials()
        bucket_name = self.bucket_entry.get()
        
        # Clear log output
        self.log_output.delete(1.0, tk.END)
        
        # Setup logging to write to scrolledtext widget
        class TextHandler(logging.Handler):
            def __init__(self, text_widget):
                logging.Handler.__init__(self)
                self.text_widget = text_widget

            def emit(self, record):
                msg = self.format(record)
                self.text_widget.insert(tk.END, msg + '\n')
                self.text_widget.see(tk.END)

        # Configure logging
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        text_handler = TextHandler(self.log_output)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        text_handler.setFormatter(formatter)
        logger.addHandler(text_handler)

        # Run your upload script here
        try:
            # This is where you'd call your upload function
            logging.info(f"Starting upload to bucket: {bucket_name}")
            # Example: archiveWithLinks(bucket_name, creds)
            logging.info("Upload completed successfully")
        except Exception as e:
            logging.error(f"Error during upload: {str(e)}")

    def run_create_url_fields(self):
        creds = self.get_credentials()
        
        # Clear status output
        self.url_status_output.delete(1.0, tk.END)
        
        try:
            self.create_url_fields(creds)
            self.url_status_output.insert(tk.END, "URL fields created successfully!\n")
        except Exception as e:
            self.url_status_output.insert(tk.END, f"Error: {str(e)}\n")

    def create_url_fields(self, creds):
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        headers = {
            'QB-Realm-Hostname': creds['qb_realm_hostname'],
            'Authorization': f'QB-USER-TOKEN {creds["qb_user_token"]}'
        }

        def get_tables():
            params = {'appId': creds['app_id']}
            r = requests.get('https://api.quickbase.com/v1/tables', 
                            params=params, headers=headers, verify=False)
            return r.json()

        def get_fields(table_id):
            params = {'tableId': table_id}
            r = requests.get('https://api.quickbase.com/v1/fields', 
                            params=params, headers=headers, verify=False)
            return r.json()

        def create_field(table_id, field_label, field_type):
            params = {'tableId': table_id}
            body = {'label': field_label, 'fieldType': field_type}
            r = requests.post('https://api.quickbase.com/v1/fields', 
                             params=params, headers=headers, verify=False, json=body)
            return r.json()

        for table in get_tables():
            self.url_status_output.insert(tk.END, f"Processing table: {table['name']}\n")
            self.update_idletasks()
            
            fields = get_fields(table["id"])
            for field in fields:
                if field['fieldType'] == "file":
                    new_field_label = f"{field['label']}_ARCHIVE"
                    new_field = create_field(table["id"], new_field_label, "url")
                    
                    update_field_params = {
                        'tableId': table["id"],
                        'fieldId': new_field['id'],
                        'properties': {'linkedToFieldId': field['id']}
                    }
                    requests.put('https://api.quickbase.com/v1/fields', 
                                params=update_field_params, headers=headers, 
                                verify=False, json=update_field_params['properties'])
                    
                    self.url_status_output.insert(tk.END, 
                        f"Created URL field '{new_field_label}' for file field '{field['label']}'\n")
                    self.update_idletasks()

if __name__ == "__main__":
    app = QuickbaseArchiveApp()
    app.mainloop()