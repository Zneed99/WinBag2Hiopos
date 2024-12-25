import os
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from export import export_action


class FileRenameHandler(FileSystemEventHandler):
    def __init__(self, folder_to_watch, required_files, action_function):
        self.folder_to_watch = folder_to_watch
        self.required_files = required_files
        self.action_function = action_function
        self.processed = False  # Flag to prevent duplicate processing

    def _all_files_present(self):
        """Check if all required files are present."""
        current_files = set(os.listdir(self.folder_to_watch))
        return all(req_file in current_files for req_file in self.required_files)

    def _process_files(self):
        """Trigger the action if all required files are present."""
        if not self.processed and self._all_files_present():
            print("All required files are present. Processing...")
            file_paths = [
                os.path.join(self.folder_to_watch, file) for file in self.required_files
            ]
            self.action_function(file_paths)
            self.processed = True  # Mark as processed
        else:
            print("Waiting for required files...")

    def on_created(self, event):
        if not event.is_directory:
            # print(f"File created: {event.src_path}")
            self._process_files()

    def on_modified(self, event):
        if not event.is_directory:
            # print(f"File modified: {event.src_path}")
            self._process_files()


def custom_action(file_paths):
    export_action(file_paths)
    # print(f"Custom action triggered for files: {file_paths}")


def monitor_folder(folder_to_watch, required_files):
    event_handler = FileRenameHandler(folder_to_watch, required_files, custom_action)
    observer = Observer()
    observer.schedule(event_handler, folder_to_watch, recursive=False)
    observer.start()
    print(f"Monitoring folder: {folder_to_watch}")

    try:
        while True:
            time.sleep(5)  # Keep the script running
    except KeyboardInterrupt:
        observer.stop()
        print("Stopped monitoring.")
    observer.join()


if __name__ == "__main__":
    # Define the folder to watch and the required files
    folder_to_watch = (
        "C:/Users/FelixHolmesten/OneDrive - LexEnergy/Skrivbordet/InstallSystemService"
    )
    required_files = ["Försäljning.csv", "Betalsätt.csv", "Följesedlar.csv"]

    if os.path.exists(folder_to_watch):
        monitor_folder(folder_to_watch, required_files)
    else:
        print("The specified folder does not exist.")
