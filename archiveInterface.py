import tkinter as tk
from tkinter import ttk, scrolledtext
import requests
import urllib3
import json
import boto3
import concurrent.futures
import logging
from typing import Dict, Any
import time
import threading

class QuickbaseArchiveApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("ProTrack Archive Tool")
        self.geometry("1200x800")  # Larger default size
        
        # Add dark title bar (Windows only)
        try:
            # Windows-specific dark title bar
            from ctypes import windll, byref, sizeof, c_int
            DWMWA_USE_IMMERSIVE_DARK_MODE = 20
            windll.dwmapi.DwmSetWindowAttribute(
                self.winfo_id(), 
                DWMWA_USE_IMMERSIVE_DARK_MODE,
                byref(c_int(2)), 
                sizeof(c_int)
            )
        except:
            pass  # Silently fail on other operating systems
        
        # Modern color scheme with softer colors
        self.COLORS = {
            'bg': '#2b2b2b',           # Softer dark background
            'primary': '#2962ff',      # Blue accent
            'secondary': '#313335',    # Slightly lighter dark for inputs
            'text': '#ffffff',         # White text
            'error': '#ff6b68',        # Softer red for errors
            'success': '#c3e88d',      # Soft green for success
            'warning': '#ffcb6b',      # Soft yellow for warnings
            'info': '#82aaff',         # Soft blue for info
            'timestamp': '#808080',    # Gray for timestamps
            'border': '#404040',       # Lighter gray for borders
            'separator': '#404040'     # Color for separators/borders
        }
        
        # Center the window on screen
        screen_width = self.winfo_screenwidth()
        screen_height = self.winfo_screenheight()
        x = (screen_width - 1200) // 2
        y = (screen_height - 800) // 2
        self.geometry(f"1200x800+{x}+{y}")
        
        self.configure(bg=self.COLORS['bg'])
        
        # Configure modern styling
        self.style = ttk.Style(self)
        self.style.theme_use('clam')  # Using clam as base theme
        
        # Configure styles for different widgets
        self.style.configure(
            'TLabel',
            background=self.COLORS['bg'],
            foreground=self.COLORS['text'],
            font=('Segoe UI', 10)
        )
        
        self.style.configure(
            'TEntry',
            fieldbackground=self.COLORS['secondary'],
            foreground=self.COLORS['text'],
            font=('Segoe UI', 10)
        )
        
        self.style.configure(
            'TButton',
            background=self.COLORS['primary'],
            font=('Segoe UI', 10, 'bold'),
            padding=10
        )
        
        self.style.configure(
            'TNotebook',
            background=self.COLORS['bg'],
            bordercolor=self.COLORS['border'],
            darkcolor=self.COLORS['border'],
            lightcolor=self.COLORS['border'],
            tabmargins=[2, 5, 2, 0]
        )
        
        self.style.configure(
            'TNotebook.Tab',
            background=self.COLORS['secondary'],
            foreground=self.COLORS['text'],
            font=('Segoe UI', 10),
            padding=[15, 5],
            bordercolor=self.COLORS['border']
        )
        
        self.style.configure(
            'TFrame',
            background=self.COLORS['bg'],
            bordercolor=self.COLORS['border'],
            darkcolor=self.COLORS['border'],
            lightcolor=self.COLORS['border']
        )
        
        self.style.configure(
            'TLabelframe',
            background=self.COLORS['bg'],
            font=('Segoe UI', 10)
        )
        
        self.style.configure(
            'TLabelframe.Label',
            background=self.COLORS['bg'],
            font=('Segoe UI', 10, 'bold')
        )

        # Add custom button style
        self.style.configure(
            'Accent.TButton',
            background=self.COLORS['primary'],
            foreground='white',
            padding=[20, 10],
            font=('Segoe UI', 10, 'bold')
        )
        
        # Configure button hover effect
        self.style.map('Accent.TButton',
            background=[('active', '#1976d2')],  # Darker blue on hover
            foreground=[('active', 'white')]
        )

        # Add custom progress bar style
        self.style.configure(
            'Custom.Horizontal.TProgressbar',
            troughcolor=self.COLORS['bg'],          # Dark background
            background=self.COLORS['primary'],       # Blue progress
            darkcolor=self.COLORS['primary'],        # Consistent color
            lightcolor=self.COLORS['primary'],       # Consistent color
            bordercolor=self.COLORS['secondary'],    # Border color
            thickness=20                             # Thicker bar
        )

        # Configure LabelFrame style
        self.style.configure(
            'Custom.TLabelframe',
            background=self.COLORS['bg'],
            bordercolor=self.COLORS['border'],  # Lighter border
            darkcolor=self.COLORS['border'],    # Consistent border color
            lightcolor=self.COLORS['border']    # Consistent border color
        )

        # Configure LabelFrame.Label style (for the "Credentials" text)
        self.style.configure(
            'Custom.TLabelframe.Label',
            background=self.COLORS['bg'],
            foreground=self.COLORS['text'],  # White text
            font=('Segoe UI', 10, 'bold')
        )

        # Update the Toggle button style for better visibility
        self.style.configure(
            'Toggle.TButton',
            background=self.COLORS['secondary'],
            foreground=self.COLORS['text'],  # White text
            font=('Segoe UI', 8),
            padding=2,
            bordercolor=self.COLORS['border']
        )

        # Add hover effect for Toggle buttons
        self.style.map('Toggle.TButton',
            background=[('active', self.COLORS['primary'])],
            foreground=[('active', self.COLORS['text'])]
        )

        # Update the entry style for better borders
        self.style.configure(
            'TEntry',
            fieldbackground=self.COLORS['secondary'],
            foreground=self.COLORS['text'],
            bordercolor=self.COLORS['border'],
            darkcolor=self.COLORS['border'],
            lightcolor=self.COLORS['border'],
            selectbackground=self.COLORS['primary'],
            selectforeground=self.COLORS['text']
        )

        # Update all remaining border colors
        self.style.configure('TSeparator',
            background=self.COLORS['separator']
        )

        self.style.configure('TFrame',
            background=self.COLORS['bg'],
            bordercolor=self.COLORS['border'],
            darkcolor=self.COLORS['border'],
            lightcolor=self.COLORS['border']
        )

        # Create and pack the main container
        self.main_container = ttk.Frame(self)
        self.main_container.pack(expand=True, fill="both", padx=20, pady=20)

        # Create notebook with modern styling
        self.notebook = ttk.Notebook(self.main_container)
        self.notebook.pack(expand=True, fill="both")

        # Initialize tabs
        self.upload_tab = ttk.Frame(self.notebook)
        self.url_fields_tab = ttk.Frame(self.notebook)
        self.delete_tab = ttk.Frame(self.notebook)
        
        self.notebook.add(self.upload_tab, text="Upload Files")
        self.notebook.add(self.url_fields_tab, text="Create URL Fields")
        self.notebook.add(self.delete_tab, text="Delete Files")

        # Initialize shared_entries before using it
        self.shared_entries = {}
        self.show_buttons = {}

        # Initialize components
        self.create_shared_credentials_frame()
        self.create_upload_tab()
        self.create_url_fields_tab()
        self.create_delete_tab()

        # Replace boolean flags with Events for better control
        self.pause_event = threading.Event()
        self.cancel_event = threading.Event()
        
        # Initialize events as cleared
        self.pause_event.clear()
        self.cancel_event.clear()

        # Now update the entry borders after shared_entries is initialized
        for entry in self.shared_entries.values():
            entry.configure(
                highlightbackground=self.COLORS['border'],
                highlightcolor=self.COLORS['primary'],
                highlightthickness=1,
                bd=0
            )

        # Update Notebook selected tab appearance
        self.style.map('TNotebook.Tab',
            background=[
                ('selected', self.COLORS['secondary']),
                ('active', self.COLORS['primary'])
            ],
            foreground=[
                ('selected', self.COLORS['text']),
                ('active', self.COLORS['text'])
            ],
            bordercolor=[
                ('selected', self.COLORS['primary']),
                ('active', self.COLORS['border'])
            ]
        )

        # Update button borders
        self.style.configure('TButton',
            bordercolor=self.COLORS['border'],
            darkcolor=self.COLORS['border'],
            lightcolor=self.COLORS['border']
        )

        self.style.configure('Accent.TButton',
            bordercolor=self.COLORS['primary'],
            darkcolor=self.COLORS['primary'],
            lightcolor=self.COLORS['primary']
        )

        # Update scrollbar style
        self.style.configure('Vertical.TScrollbar',
            background=self.COLORS['secondary'],
            bordercolor=self.COLORS['border'],
            arrowcolor=self.COLORS['text'],
            troughcolor=self.COLORS['bg']
        )

        self.style.configure('Horizontal.TScrollbar',
            background=self.COLORS['secondary'],
            bordercolor=self.COLORS['border'],
            arrowcolor=self.COLORS['text'],
            troughcolor=self.COLORS['bg']
        )

        # Update scrollbar mapping for hover effects
        self.style.map('Vertical.TScrollbar',
            background=[('active', self.COLORS['primary'])]
        )

        self.style.map('Horizontal.TScrollbar',
            background=[('active', self.COLORS['primary'])]
        )

    def create_shared_credentials_frame(self):
        # Create a simple frame without scrolling
        cred_frame = ttk.LabelFrame(
            self.main_container,
            text="Credentials",
            padding=15,
            style='Custom.TLabelframe'  # Add custom style
        )
        cred_frame.pack(fill="x", pady=(0, 10))

        # Streamlined credentials setup
        shared_fields = [
            ("AWS Access Key:", "aws_access_key", False, "AKIAXR5GICGZ6SD2QUXY"),
            ("AWS Secret Key:", "aws_secret_key", True, "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"),
            ("QB Realm Hostname:", "qb_realm_hostname", False, "protrack"),
            ("QB User Token:", "qb_user_token", True, "b5w6yk_v5c_0_ctwp43wb8p6hrzbjmn4h6bgrch5x"),
            ("QB App ID:", "app_id", False, "bi54diz7v")
        ]

        # Configure show/hide button style
        self.style.configure(
            'Toggle.TButton',
            background=self.COLORS['secondary'],
            font=('Segoe UI', 8),
            padding=2
        )

        for i, (label_text, field_key, is_secret, default_value) in enumerate(shared_fields):
            # Label
            ttk.Label(cred_frame, text=label_text).grid(row=i, column=0, sticky="e", padx=(0, 10), pady=5)
            
            # Entry field - using tk.Entry instead of ttk.Entry
            entry = tk.Entry(
                cred_frame,
                show="*" if is_secret else "",
                font=('Segoe UI', 10),
                bg=self.COLORS['secondary'],
                fg=self.COLORS['text'],  # White text
                insertbackground=self.COLORS['text'],  # White cursor
                relief=tk.FLAT
            )
            entry.insert(0, default_value)
            entry.grid(row=i, column=1, sticky="ew", pady=5)
            self.shared_entries[field_key] = entry

            # Show/Hide button for sensitive fields
            if is_secret:
                show_var = tk.BooleanVar(value=False)
                show_button = ttk.Button(
                    cred_frame,
                    text="Show",
                    style='Toggle.TButton',
                    width=6,
                    command=lambda e=entry, v=show_var: self.toggle_show_password(e, v)
                )
                show_button.grid(row=i, column=2, padx=(5, 0), pady=5)
                self.show_buttons[field_key] = (show_button, show_var)

        cred_frame.columnconfigure(1, weight=1)

    def toggle_show_password(self, entry_widget, show_var):
        """Toggle between showing and hiding the password"""
        current_state = show_var.get()
        show_var.set(not current_state)
        entry_widget.configure(show="" if not current_state else "*")
        
        # Update button text
        button = [btn for btn, var in self.show_buttons.values() if var == show_var][0]
        button.configure(text="Hide" if not current_state else "Show")

    def create_upload_tab(self):
        frame = ttk.Frame(self.upload_tab, padding=20)
        frame.pack(fill=tk.BOTH, expand=True)

        # S3 bucket input with modern styling
        input_frame = ttk.Frame(frame)
        input_frame.pack(fill=tk.X, pady=(0, 20))
        
        ttk.Label(input_frame, text="S3 Bucket Name").pack(anchor='w', pady=(0, 5))
        self.bucket_entry = tk.Entry(
            input_frame,
            font=('Segoe UI', 10),
            bg=self.COLORS['secondary'],
            fg=self.COLORS['text'],  # White text
            insertbackground=self.COLORS['text'],  # White cursor
            relief=tk.FLAT
        )
        self.bucket_entry.insert(0, 'canes-protrack')
        self.bucket_entry.pack(fill=tk.X)

        # Progress frame
        progress_frame = ttk.Frame(frame)
        progress_frame.pack(fill=tk.X, pady=(0, 10))
        
        # Progress bar
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(
            progress_frame, 
            variable=self.progress_var,
            maximum=100,
            mode='determinate',
            style='Custom.Horizontal.TProgressbar'
        )
        self.progress_bar.pack(fill=tk.X, pady=(0, 5))
        
        # Progress label
        self.progress_label = ttk.Label(progress_frame, text="")
        self.progress_label.pack(anchor='w')

        # Control buttons frame
        control_frame = ttk.Frame(frame)
        control_frame.pack(fill=tk.X, pady=(0, 20))
        
        # Upload button
        self.upload_button = ttk.Button(
            control_frame,
            text="Start Upload",
            command=self.run_upload,
            style='Accent.TButton'
        )
        self.upload_button.pack(side=tk.LEFT, padx=5)

        # Pause button
        self.pause_button = ttk.Button(
            control_frame,
            text="Pause",
            command=self.toggle_pause,
            state='disabled',
            style='Accent.TButton'
        )
        self.pause_button.pack(side=tk.LEFT, padx=5)

        # Cancel button
        self.cancel_button = ttk.Button(
            control_frame,
            text="Cancel",
            command=self.cancel_upload,
            state='disabled',
            style='Accent.TButton'
        )
        self.cancel_button.pack(side=tk.LEFT, padx=5)

        # Terminal-style log output
        ttk.Label(frame, text="Console Output").pack(anchor='w', pady=(0, 5))
        
        # Create a frame for the terminal
        terminal_frame = ttk.Frame(frame, style='Terminal.TFrame')
        terminal_frame.pack(fill=tk.BOTH, expand=True)
        
        # Configure terminal style
        self.style.configure('Terminal.TFrame', background=self.COLORS['secondary'])
        
        # Create the terminal-like text widget
        self.log_output = scrolledtext.ScrolledText(
            terminal_frame,
            height=12,
            font=('Consolas', 10),  # Use a monospace font
            background=self.COLORS['secondary'],  # Lighter background
            foreground=self.COLORS['success'],    # Default text color
            insertbackground=self.COLORS['text'], # Cursor color
            relief=tk.FLAT,
            padx=10,
            pady=10
        )
        self.log_output.pack(fill=tk.BOTH, expand=True)
        
        # Configure tag colors for different log levels
        self.log_output.tag_configure('INFO', foreground=self.COLORS['info'])
        self.log_output.tag_configure('ERROR', foreground=self.COLORS['error'])
        self.log_output.tag_configure('WARNING', foreground=self.COLORS['warning'])
        self.log_output.tag_configure('SUCCESS', foreground=self.COLORS['success'])
        self.log_output.tag_configure('TIMESTAMP', foreground=self.COLORS['timestamp'])

    def update_progress(self, current, total, message="", phase=None):
        """Update progress bar and label with phase information"""
        if total > 0:
            if phase:
                # For phase-based progress, scale the percentage to the appropriate range
                phase_ranges = {
                    'scanning': (0, 20),    # First 20% for scanning files
                    'preparing': (20, 30),  # Next 10% for preparation
                    'uploading': (30, 90),  # 60% for actual upload
                    'updating': (90, 100)   # Last 10% for QB updates
                }
                
                if phase in phase_ranges:
                    start_percent, end_percent = phase_ranges[phase]
                    phase_percentage = (current / total) * (end_percent - start_percent)
                    actual_percentage = start_percent + phase_percentage
                    
                    # Smooth progress update
                    current_value = self.progress_var.get()
                    if actual_percentage > current_value:
                        step = min(1.0, actual_percentage - current_value)  # Max 1% increment
                        self.progress_var.set(current_value + step)
                    
                    self.progress_label.config(text=f"{phase.title()}: {message} ({current}/{total})")
            else:
                # Traditional single-phase progress
                percentage = (current / total) * 100
                current_value = self.progress_var.get()
                if percentage > current_value:
                    step = min(1.0, percentage - current_value)  # Max 1% increment
                    self.progress_var.set(current_value + step)
                self.progress_label.config(text=f"{message} ({current}/{total})")
        self.update_idletasks()

    def create_url_fields_tab(self):
        frame = ttk.Frame(self.url_fields_tab, padding=20)
        frame.pack(fill=tk.BOTH, expand=True)

        # Progress frame
        progress_frame = ttk.Frame(frame)
        progress_frame.pack(fill=tk.X, pady=(0, 10))
        
        # Progress bar
        self.url_progress_var = tk.DoubleVar()
        self.url_progress_bar = ttk.Progressbar(
            progress_frame, 
            variable=self.url_progress_var,
            maximum=100,
            mode='determinate',
            style='Custom.Horizontal.TProgressbar'
        )
        self.url_progress_bar.pack(fill=tk.X, pady=(0, 5))
        
        # Progress label
        self.url_progress_label = ttk.Label(progress_frame, text="")
        self.url_progress_label.pack(anchor='w')

        # Modern create fields button
        self.create_fields_button = ttk.Button(
            frame,
            text="Create Archive URL Fields",
            command=self.run_create_url_fields,
            style='Accent.TButton'
        )
        self.create_fields_button.pack(pady=(0, 20))

        # Status output with modern styling
        ttk.Label(frame, text="Status").pack(anchor='w', pady=(0, 5))
        self.url_status_output = scrolledtext.ScrolledText(
            frame,
            height=12,
            font=('Consolas', 10),
            background=self.COLORS['secondary'],
            foreground=self.COLORS['success'],
            insertbackground=self.COLORS['text'],
            relief=tk.FLAT,
            padx=10,
            pady=10
        )
        self.url_status_output.pack(fill=tk.BOTH, expand=True)

        # Configure tag colors for different message types
        self.url_status_output.tag_configure('ERROR', foreground=self.COLORS['error'])
        self.url_status_output.tag_configure('SUCCESS', foreground=self.COLORS['success'])
        self.url_status_output.tag_configure('INFO', foreground=self.COLORS['info'])
        self.url_status_output.tag_configure('WARNING', foreground=self.COLORS['warning'])

    def update_url_progress(self, current, total, message="", phase=None):
        """Update URL fields progress bar and label"""
        if total > 0:
            if phase:
                # For phase-based progress, scale the percentage to the appropriate range
                phase_ranges = {
                    'scanning': (0, 20),    # First 20% for scanning tables
                    'creating': (20, 90),   # 70% for creating fields
                    'updating': (90, 100)   # Last 10% for final updates
                }
                
                if phase in phase_ranges:
                    start_percent, end_percent = phase_ranges[phase]
                    phase_percentage = (current / total) * (end_percent - start_percent)
                    actual_percentage = start_percent + phase_percentage
                    self.url_progress_var.set(actual_percentage)
                    self.url_progress_label.config(text=f"{phase.title()}: {message} ({current}/{total})")
            else:
                # Traditional single-phase progress
                percentage = (current / total) * 100
                self.url_progress_var.set(percentage)
                self.url_progress_label.config(text=f"{message} ({current}/{total})")
        self.update_idletasks()

    def get_credentials(self) -> Dict[str, str]:
        return {key: entry.get() for key, entry in self.shared_entries.items()}

    def run_upload(self):
        # Reset control events
        self.pause_event.clear()
        self.cancel_event.clear()
        
        creds = self.get_credentials()
        bucket_name = self.bucket_entry.get()
        
        # Clear log output
        self.log_output.delete(1.0, tk.END)
        
        # Setup logging to write to scrolledtext widget with colors
        class ColoredTextHandler(logging.Handler):
            def __init__(self, text_widget):
                logging.Handler.__init__(self)
                self.text_widget = text_widget

            def emit(self, record):
                # Split the message into timestamp and rest
                msg = self.format(record)
                try:
                    timestamp, rest = msg.split(' - ', 1)
                except ValueError:
                    timestamp, rest = '', msg

                # Insert with appropriate tags
                self.text_widget.insert(tk.END, timestamp + ' - ', 'TIMESTAMP')
                
                # Color based on log level
                if record.levelname == 'ERROR':
                    self.text_widget.insert(tk.END, rest + '\n', 'ERROR')
                elif record.levelname == 'WARNING':
                    self.text_widget.insert(tk.END, rest + '\n', 'WARNING')
                elif 'success' in rest.lower() or 'completed' in rest.lower():
                    self.text_widget.insert(tk.END, rest + '\n', 'SUCCESS')
                else:
                    self.text_widget.insert(tk.END, rest + '\n', 'INFO')
                
                self.text_widget.see(tk.END)
                self.text_widget.update_idletasks()

        # Configure logging
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        # Remove existing handlers
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        text_handler = ColoredTextHandler(self.log_output)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        text_handler.setFormatter(formatter)
        logger.addHandler(text_handler)

        # Disable the upload button while processing
        self.upload_button.configure(state='disabled')
        
        try:
            # Create a thread to run the archive process
            def archive_thread():
                try:
                    from createFoldersInS3 import archiveWithLinks
                    logging.info(f"Starting upload to bucket: {bucket_name}")
                    
                    # Enable control buttons
                    self.pause_button.configure(state='normal')
                    self.cancel_button.configure(state='normal')
                    
                    total_files = archiveWithLinks(
                        bucket_name, 
                        credentials=creds, 
                        progress_callback=self.update_progress,
                        control_callback=self.check_controls
                    )
                    
                    if self.cancel_event.is_set():
                        logging.warning("Upload cancelled")
                    else:
                        logging.info(f"Archive completed. Total files uploaded: {total_files}")
                        
                except Exception as e:
                    logging.error(f"Error during upload: {str(e)}")
                finally:
                    # Reset and disable control buttons
                    self.upload_button.configure(state='normal')
                    self.pause_button.configure(state='disabled', text="Pause")
                    self.cancel_button.configure(state='disabled')
                    self.progress_var.set(0)
                    self.progress_label.config(text="")
                    self.is_paused = False
                    self.should_cancel = False
            
            import threading
            thread = threading.Thread(target=archive_thread)
            thread.daemon = True
            thread.start()
            
        except Exception as e:
            logging.error(f"Error starting upload thread: {str(e)}")
            self.upload_button.configure(state='normal')

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
        
        # Reset progress
        self.url_progress_var.set(0)
        self.url_progress_label.config(text="")

        headers = {
            'QB-Realm-Hostname': f"{creds['qb_realm_hostname']}.quickbase.com",  # Add .quickbase.com
            'Authorization': f'QB-USER-TOKEN {creds["qb_user_token"]}',
            'Content-Type': 'application/json'  # Add content-type header
        }

        def get_tables():
            params = {'appId': creds['app_id']}
            r = requests.get('https://api.quickbase.com/v1/tables', 
                            params=params, headers=headers, verify=False)
            if not r.ok:
                raise Exception(f"Failed to get tables: {r.text}")
            return r.json()

        def get_fields(table_id):
            r = requests.get(f'https://api.quickbase.com/v1/fields?tableId={table_id}', 
                            headers=headers, verify=False)
            if not r.ok:
                raise Exception(f"Failed to get fields: {r.text}")
            return r.json()

        def create_field(table_id, field_label, field_type):
            body = {
                'label': field_label,
                'fieldType': field_type,
                'addToForms': True
            }
            r = requests.post(f'https://api.quickbase.com/v1/fields?tableId={table_id}', 
                             headers=headers, verify=False, json=body)
            if not r.ok:
                raise Exception(f"Failed to create field: {r.text}")
            return r.json()

        try:
            # Scanning phase
            self.update_url_progress(0, 1, "Scanning tables", phase='scanning')
            tables = get_tables()
            total_file_fields = 0
            
            # Count total file fields
            for table in tables:
                fields = get_fields(table["id"])
                total_file_fields += sum(1 for field in fields if field['fieldType'] == "file")
            
            self.update_url_progress(1, 1, "Scan complete", phase='scanning')
            
            if total_file_fields == 0:
                self.url_status_output.insert(tk.END, "No file fields found in any table\n", 'WARNING')
                return

            processed_fields = 0
            for table in tables:
                self.url_status_output.insert(tk.END, f"Processing table: {table['name']}\n", 'INFO')
                self.update_idletasks()
                
                fields = get_fields(table["id"])
                file_fields_found = False
                
                for field in fields:
                    if field['fieldType'] == "file":
                        file_fields_found = True
                        new_field_label = f"{field['label']}_ARCHIVE"
                        
                        # Check if field already exists
                        if not any(f['label'] == new_field_label for f in fields):
                            try:
                                self.update_url_progress(processed_fields, total_file_fields, 
                                                       f"Creating field in {table['name']}", phase='creating')
                                
                                new_field = create_field(table["id"], new_field_label, "url")
                                
                                self.update_url_progress(processed_fields, total_file_fields, 
                                                       f"Updating field in {table['name']}", phase='updating')
                                
                                # Update field properties to link it
                                update_body = {
                                    'properties': {
                                        'linkedToFieldId': field['id']
                                    }
                                }
                                update_r = requests.post(
                                    f'https://api.quickbase.com/v1/fields/{new_field["id"]}?tableId={table["id"]}',
                                    headers=headers,
                                    verify=False,
                                    json=update_body
                                )
                                if not update_r.ok:
                                    raise Exception(f"Failed to link field: {update_r.text}")
                                
                                self.url_status_output.insert(tk.END, 
                                    f"Created URL field '{new_field_label}' for file field '{field['label']}'\n", 'SUCCESS')
                            except Exception as e:
                                self.url_status_output.insert(tk.END, 
                                    f"Error creating field '{new_field_label}': {str(e)}\n", 'ERROR')
                        else:
                            self.url_status_output.insert(tk.END, 
                                f"URL field '{new_field_label}' already exists - skipping\n", 'WARNING')
                        
                        processed_fields += 1
                        self.update_idletasks()
                
                if not file_fields_found:
                    self.url_status_output.insert(tk.END, 
                        f"No file fields found in table '{table['name']}'\n", 'INFO')
            
            # Set progress to 100% when complete
            self.update_url_progress(total_file_fields, total_file_fields, "Complete", phase='updating')
                
        except Exception as e:
            self.url_status_output.insert(tk.END, f"Error: {str(e)}\n", 'ERROR')
            raise

    def toggle_pause(self):
        """Toggle pause state with immediate effect"""
        if self.pause_event.is_set():
            self.pause_event.clear()
            self.pause_button.configure(text="Pause")
            logging.info("Upload resumed")
        else:
            self.pause_event.set()
            self.pause_button.configure(text="Resume")
            logging.info("Upload paused")

    def cancel_upload(self):
        """Cancel the upload process with immediate effect"""
        self.cancel_event.set()
        self.pause_event.clear()  # Clear pause if set
        logging.warning("Cancelling upload...")

    def check_controls(self):
        """Check if process should pause or cancel"""
        if self.cancel_event.is_set():
            return True
        
        if self.pause_event.is_set():
            # Wait until resumed or cancelled
            while self.pause_event.is_set() and not self.cancel_event.is_set():
                time.sleep(0.1)  # Prevent CPU hogging
            
        return self.cancel_event.is_set()

    def create_delete_tab(self):
        frame = ttk.Frame(self.delete_tab, padding=20)
        frame.pack(fill=tk.BOTH, expand=True)

        # Progress frame
        progress_frame = ttk.Frame(frame)
        progress_frame.pack(fill=tk.X, pady=(0, 10))
        
        # Progress bar
        self.delete_progress_var = tk.DoubleVar()
        self.delete_progress_bar = ttk.Progressbar(
            progress_frame, 
            variable=self.delete_progress_var,
            maximum=100,
            mode='determinate',
            style='Custom.Horizontal.TProgressbar'
        )
        self.delete_progress_bar.pack(fill=tk.X, pady=(0, 5))
        
        # Progress label
        self.delete_progress_label = ttk.Label(progress_frame, text="")
        self.delete_progress_label.pack(anchor='w')

        # Control buttons frame
        control_frame = ttk.Frame(frame)
        control_frame.pack(fill=tk.X, pady=(0, 20))
        
        # Delete button
        self.delete_button = ttk.Button(
            control_frame,
            text="Start Delete",
            command=self.run_delete,
            style='Accent.TButton'
        )
        self.delete_button.pack(side=tk.LEFT, padx=5)

        # Pause button
        self.delete_pause_button = ttk.Button(
            control_frame,
            text="Pause",
            command=self.toggle_delete_pause,
            state='disabled',
            style='Accent.TButton'
        )
        self.delete_pause_button.pack(side=tk.LEFT, padx=5)

        # Cancel button
        self.delete_cancel_button = ttk.Button(
            control_frame,
            text="Cancel",
            command=self.cancel_delete,
            state='disabled',
            style='Accent.TButton'
        )
        self.delete_cancel_button.pack(side=tk.LEFT, padx=5)

        # Terminal-style log output
        ttk.Label(frame, text="Console Output").pack(anchor='w', pady=(0, 5))
        
        # Create a frame for the terminal
        terminal_frame = ttk.Frame(frame, style='Terminal.TFrame')
        terminal_frame.pack(fill=tk.BOTH, expand=True)
        
        # Create the terminal-like text widget
        self.delete_log_output = scrolledtext.ScrolledText(
            terminal_frame,
            height=12,
            font=('Consolas', 10),
            background=self.COLORS['secondary'],
            foreground=self.COLORS['success'],
            insertbackground=self.COLORS['text'],
            relief=tk.FLAT,
            padx=10,
            pady=10
        )
        self.delete_log_output.pack(fill=tk.BOTH, expand=True)
        
        # Configure tag colors for different log levels
        self.delete_log_output.tag_configure('INFO', foreground=self.COLORS['info'])
        self.delete_log_output.tag_configure('ERROR', foreground=self.COLORS['error'])
        self.delete_log_output.tag_configure('WARNING', foreground=self.COLORS['warning'])
        self.delete_log_output.tag_configure('SUCCESS', foreground=self.COLORS['success'])
        self.delete_log_output.tag_configure('TIMESTAMP', foreground=self.COLORS['timestamp'])

    def run_delete(self):
        creds = self.get_credentials()
        
        # Clear status output
        self.delete_log_output.delete(1.0, tk.END)
        
        try:
            self.delete_url_fields(creds)
            self.delete_log_output.insert(tk.END, "URL fields deleted successfully!\n")
        except Exception as e:
            self.delete_log_output.insert(tk.END, f"Error: {str(e)}\n")

    def delete_url_fields(self, creds):
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        headers = {
            'QB-Realm-Hostname': f"{creds['qb_realm_hostname']}.quickbase.com",  # Add .quickbase.com
            'Authorization': f'QB-USER-TOKEN {creds["qb_user_token"]}',
            'Content-Type': 'application/json'  # Add content-type header
        }

        def get_tables():
            params = {'appId': creds['app_id']}
            r = requests.get('https://api.quickbase.com/v1/tables', 
                            params=params, headers=headers, verify=False)
            if not r.ok:
                raise Exception(f"Failed to get tables: {r.text}")
            return r.json()

        def get_fields(table_id):
            r = requests.get(f'https://api.quickbase.com/v1/fields?tableId={table_id}', 
                            headers=headers, verify=False)
            if not r.ok:
                raise Exception(f"Failed to get fields: {r.text}")
            return r.json()

        def delete_field(table_id, field_label):
            body = {
                'label': field_label,
                'fieldType': "url"
            }
            r = requests.delete(f'https://api.quickbase.com/v1/fields?tableId={table_id}&fieldLabel={field_label}', 
                               headers=headers, verify=False, json=body)
            if not r.ok:
                raise Exception(f"Failed to delete field: {r.text}")
            return r.json()

        try:
            tables = get_tables()
            for table in tables:
                self.delete_log_output.insert(tk.END, f"Processing table: {table['name']}\n", 'INFO')
                self.update_idletasks()
                
                fields = get_fields(table["id"])
                file_fields_found = False
                
                for field in fields:
                    if field['fieldType'] == "url":
                        file_fields_found = True
                        new_field_label = f"{field['label']}_ARCHIVE"
                        
                        # Check if field already exists
                        if not any(f['label'] == new_field_label for f in fields):
                            try:
                                delete_field(table["id"], new_field_label)
                                
                                self.delete_log_output.insert(tk.END, 
                                    f"Deleted URL field '{new_field_label}' for file field '{field['label']}'\n", 'SUCCESS')
                            except Exception as e:
                                self.delete_log_output.insert(tk.END, 
                                    f"Error deleting field '{new_field_label}': {str(e)}\n", 'ERROR')
                        else:
                            self.delete_log_output.insert(tk.END, 
                                f"URL field '{new_field_label}' already exists - skipping\n", 'WARNING')
                        
                        self.update_idletasks()
                
                if not file_fields_found:
                    self.delete_log_output.insert(tk.END, 
                        f"No URL fields found in table '{table['name']}'\n", 'INFO')
                    
        except Exception as e:
            self.delete_log_output.insert(tk.END, f"Error: {str(e)}\n", 'ERROR')
            raise

    def toggle_delete_pause(self):
        """Toggle pause state"""
        self.is_paused = not self.is_paused
        self.delete_pause_button.configure(text="Resume" if self.is_paused else "Pause")
        if self.is_paused:
            logging.info("Delete paused")
        else:
            logging.info("Delete resumed")

    def cancel_delete(self):
        """Cancel the delete process"""
        self.should_cancel = True
        self.is_paused = False
        logging.warning("Cancelling delete...")

if __name__ == "__main__":
    app = QuickbaseArchiveApp()
    app.mainloop()