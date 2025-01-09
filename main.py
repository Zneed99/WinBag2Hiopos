import os
import shutil
import time
import pytz
import traceback
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from export import export_action
from import_ import import_action


def move_files_to_old_folder(file_paths, folder_to_watch):
    """Move the processed files to the 'Old Files' folder with a timestamp in the name."""
    old_folder_path = os.path.join(folder_to_watch, "Old Files")

    # Create the 'Old Files' folder if it doesn't exist
    if not os.path.exists(old_folder_path):
        os.makedirs(old_folder_path)
        print(f"Created 'Old Files' folder at {old_folder_path}")

    stockholm_tz = pytz.timezone("Europe/Stockholm")
    current_time = datetime.now(stockholm_tz).strftime("%Y%m%d-%H-%M-%S")

    # Move each processed file to the 'Old Files' folder with the new name
    for file_path in file_paths:
        file_name = os.path.basename(file_path)
        file_name_parts = os.path.splitext(file_name)
        new_file_name = f"{file_name_parts[0]}_{current_time}_old{file_name_parts[1]}"

        old_file_path = os.path.join(old_folder_path, new_file_name)
        shutil.move(file_path, old_file_path)
        print(f"Moved {file_name} to 'Old Files' as {new_file_name}.")


import traceback

def custom_export_action(file_paths, folder_to_watch):
    """Wrap export_action in try-except with logging/messaging."""
    try:
        print("Performing export...")
        export_action(file_paths)
        move_files_to_old_folder(file_paths, folder_to_watch)
    except Exception as e:
        # Capture the full traceback
        tb = traceback.format_exc()
        print(f"An error occurred during export_action:\n{tb}")



def custom_import_action(file_path, folder_to_watch):
    """Wrap import_action in try-except with logging/messaging."""
    try:
        print("Performing import...")
        import_action(file_path)
        move_files_to_old_folder(file_path, folder_to_watch)
    except Exception as e:
        # Log or handle the error
        tb = traceback.format_exc()
        print(f"An error occurred during import_action: {tb}")


class FileRenameHandler(FileSystemEventHandler):
    def __init__(self, folder_to_watch, export_required_files, import_required_file):
        """
        :param folder_to_watch: Folder being monitored
        :param export_required_files: List of files for export (Försäljning, Betalsätt, Följesedlar)
        :param import_required_file: Name of the file for import (PCS)
        """
        self.folder_to_watch = folder_to_watch
        self.export_required_files = export_required_files
        self.import_required_file = import_required_file
        print(f"Initialized FileRenameHandler instance: {id(self)}")

    def _all_export_files_present(self):
        """Check if all export required files exist."""
        current_files = set(os.listdir(self.folder_to_watch))
        return all(req_file in current_files for req_file in self.export_required_files)

    def _is_import_file_present(self):
        """Check if the import file (PCS) exists."""
        return self.import_required_file in os.listdir(self.folder_to_watch)

    def _process_files(self):
        """
        Determine whether to do an export or an import:
        - If PCS is present, do import_action.
        - Else if all 3 required export files are present, do export_action.
        - Otherwise, wait.
        """
        # Import scenario
        if self._is_import_file_present():
            file_path = os.path.join(self.folder_to_watch, self.import_required_file)
            print("Detected PCS file. Starting import action.")
            custom_import_action([file_path], self.folder_to_watch)
        # Export scenario
        elif self._all_export_files_present():
            file_paths = [
                os.path.join(self.folder_to_watch, file)
                for file in self.export_required_files
            ]
            print("Detected all required export files. Starting export action.")
            custom_export_action(file_paths, self.folder_to_watch)
        else:
            # Missing either PCS or the 3 required export files
            print("Waiting for PCS or all required export files...")

    def on_created(self, event):
        if not event.is_directory:
            file_path = event.src_path
            print(f"File added: {file_path}")
            self._process_files()


def monitor_folder(folder_to_watch, export_required_files, import_required_file):
    event_handler = FileRenameHandler(
        folder_to_watch=folder_to_watch,
        export_required_files=export_required_files,
        import_required_file=import_required_file,
    )
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

    # Default to home directory + "WinBag2Hipos-SystemService"
    folder_to_watch = os.path.join(
        os.path.expanduser("~"), "WinBag2Hipos-SystemService"
    )

    export_required_files = [
        "Försäljning.csv",
        "Betalsätt.csv",
        "Följesedlar.csv",
        "Moms.csv",
    ]
    import_required_file = "PCS.ADM"

    if os.path.exists(folder_to_watch):
        print(f"Monitoring folder: {folder_to_watch}")
        monitor_folder(folder_to_watch, export_required_files, import_required_file)
    else:
        print(f"The specified folder does not exist: {folder_to_watch}. Creating it...")
        try:
            os.makedirs(folder_to_watch)
            print(f"Created folder: {folder_to_watch}")
            monitor_folder(folder_to_watch, export_required_files, import_required_file)
        except Exception as e:
            print(f"Failed to create folder {folder_to_watch}. Error: {e}")
