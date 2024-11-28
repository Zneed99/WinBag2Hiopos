import os
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class FileRenameHandler(FileSystemEventHandler):
    def __init__(self, folder_to_watch, new_name_format):
        self.folder_to_watch = folder_to_watch
        self.new_name_format = new_name_format
        self.counter = 1  # To make file names unique if needed

    def on_created(self, event):
        if not event.is_directory:
            # Get the file's full path
            file_path = event.src_path
            folder = os.path.dirname(file_path)
            file_extension = os.path.splitext(file_path)[1]

            # Construct the new file name
            new_name = f"{self.new_name_format}_{self.counter}{file_extension}"
            new_path = os.path.join(folder, new_name)

            # Rename the file
            try:
                os.rename(file_path, new_path)
                print(f"Renamed: {file_path} -> {new_path}")
                self.counter += 1
            except Exception as e:
                print(f"Error renaming file {file_path}: {e}")

def monitor_folder(folder_to_watch, new_name_format):
    event_handler = FileRenameHandler(folder_to_watch, new_name_format)
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
    #folder_to_watch = input("Enter the folder path to monitor: ").strip()
    folder_to_watch = "C:/Users/holme/OneDrive/Skrivbord/Install-Testing-System-Service"
    new_name_format = "Testing very nice"

    if os.path.exists(folder_to_watch):
        monitor_folder(folder_to_watch, new_name_format)
    else:
        print("The specified folder does not exist.")
