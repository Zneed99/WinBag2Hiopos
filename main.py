import os
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from export import (
    export_action
)

class FileRenameHandler(FileSystemEventHandler):
    def __init__(self, folder_to_watch, new_name_format, required_files, action_function):
        self.folder_to_watch = folder_to_watch
        self.new_name_format = new_name_format
        self.required_files = required_files
        self.action_function = action_function
        self.counter = 1  # To make file names unique if needed

    def on_created(self, event):
        if not event.is_directory:
            # Check if all required files are present in the folder
            current_files = set(os.listdir(self.folder_to_watch))
            print(f"Current files in folder: {current_files}")
            if all(req_file in current_files for req_file in self.required_files):
                print("All required files are present.")
                file_paths = [os.path.join(self.folder_to_watch, file) for file in self.required_files]
                self.action_function(file_paths)  # Trigger the custom action with file paths
            else:
                print("Waiting for required files...")

    def on_modified(self, event):
        self.on_created(event)  # Check for required files on modification events too

def custom_action(file_paths):
    export_action(file_paths)
    # Add the desired functionality here.


def monitor_folder(folder_to_watch, new_name_format, required_files):
    event_handler = FileRenameHandler(folder_to_watch, new_name_format, required_files, custom_action)
    observer = Observer()
    observer.schedule(event_handler, folder_to_watch, recursive=False)
    observer.start()
    print(f"Monitoring folder: {folder_to_watch}")

    try:
        while True:
            time.sleep(1)  # Keep the script running
    except KeyboardInterrupt:
        observer.stop()
        print("Stopped monitoring.")
    observer.join()

if __name__ == "__main__":
    # Define the folder to watch and the required files
    folder_to_watch = "C:/Users/holme/OneDrive/Skrivbord/Install-Testing-System-Service"
    new_name_format = "Testing very nice"
    required_files = ["Försäljning.csv", "Betalsätt.csv", "Följesedlar.csv"]

    if os.path.exists(folder_to_watch):
        monitor_folder(folder_to_watch, new_name_format, required_files)
    else:
        print("The specified folder does not exist.")
