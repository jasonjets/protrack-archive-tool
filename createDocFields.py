import json
import tkinter as tk
from tkinter import ttk
import requests
import urllib3

"""
aws_access_key = 'AKIAXR5GICGZ6SD2QUXY'
aws_secret_key = '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'
appId = "bkv4gpres"

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning) # Disable SSL warnings

headers = {
        'QB-Realm-Hostname': 'protrack.quickbase.com',
        'User-Agent': '{User-Agent}',
        'Authorization': 'QB-USER-TOKEN b5w6yk_v5c_0_ctwp43wb8p6hrzbjmn4h6bgrch5x'
    }

    
"""
def createUrlFields(aws_access_key, aws_secret_key, qb_realm_hostname, qb_user_token, app_id):
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    headers = {
        'QB-Realm-Hostname': qb_realm_hostname,
        'User-Agent': '{User-Agent}',
        'Authorization': f'QB-USER-TOKEN {qb_user_token}'
    }

    def getTables():
        params = {'appId': app_id}
        r = requests.get('https://api.quickbase.com/v1/tables', params=params, headers=headers, verify=False)
        return r.json()

    def getFields(tableID):
        params = {'tableId': tableID}
        r = requests.get('https://api.quickbase.com/v1/fields', params=params, headers=headers, verify=False)
        return r.json()

    def create_field(tableID, fieldLabel, fieldType):
        params = {'tableId': tableID}
        body = {'label': fieldLabel, 'fieldType': fieldType}
        r = requests.post('https://api.quickbase.com/v1/fields', params=params, headers=headers, verify=False, json=body)
        return json.dumps(r.json(), indent=4)

    for table in getTables():
        fields = getFields(table["id"])
        for field in fields:
            if field['fieldType'] == "file":
                file_field_id = field['id']
                new_field_label = f"{field['label']}_ARCHIVE"
                new_field_response = create_field(table["id"], new_field_label, "url")
                new_field_id = json.loads(new_field_response)['id']

                update_field_params = {
                    'tableId': table["id"],
                    'fieldId': new_field_id,
                    'properties': {'linkedToFieldId': file_field_id}
                }
                requests.put('https://api.quickbase.com/v1/fields', params=update_field_params, headers=headers, verify=False, json=update_field_params['properties'])
                print(f"Created new URL field '{new_field_label}' for table '{table['name']}'")

class QuickbaseApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("ProTrack Archive URL Fields")
        self.geometry("500x400")
        self.configure(bg="#f0f0f0")

        style = ttk.Style(self)
        style.configure('TLabel', background="#f0f0f0", font=('Helvetica', 12))
        style.configure('TEntry', font=('Helvetica', 12))
        style.configure('TButton', font=('Helvetica', 12), padding=10)

        self.create_widgets()

    def create_widgets(self):
        frame = ttk.Frame(self, padding="20")
        frame.pack(fill=tk.BOTH, expand=True)

        labels = [
            "AWS Access Key:", 
            "AWS Secret Key:", 
            "Quickbase Realm Hostname:", 
            "Quickbase User Token:", 
            "Quickbase App ID:"
        ]
        
        self.entries = {}
        for label in labels:
            ttk.Label(frame, text=label).pack(pady=5, anchor='w')
            entry = ttk.Entry(frame, show="*" if "Secret" in label else "")
            entry.pack(pady=5, fill=tk.X)
            self.entries[label] = entry

        self.create_button = ttk.Button(frame, text="Create Archive URL Fields", command=self.run_createUrlFields)
        self.create_button.pack(pady=20)

        self.status_label = ttk.Label(frame, text="", foreground="green")
        self.status_label.pack(pady=10)

    def run_createUrlFields(self):
        aws_access_key = self.entries["AWS Access Key:"].get()
        aws_secret_key = self.entries["AWS Secret Key:"].get()
        qb_realm_hostname = self.entries["Quickbase Realm Hostname:"].get()
        qb_user_token = self.entries["Quickbase User Token:"].get()
        app_id = self.entries["Quickbase App ID:"].get()

        createUrlFields(aws_access_key, aws_secret_key, qb_realm_hostname, qb_user_token, app_id)
        self.status_label.config(text="Archive URL fields created successfully!")

if __name__ == "__main__":
    app = QuickbaseApp()
    app.mainloop()
