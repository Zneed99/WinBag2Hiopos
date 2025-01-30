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
    old_folder_path = os.path.join("C:/winbag_export", "Old Files")

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
        :param export_required_files: List of required files for export
        :param import_required_file: Name of the file for import (PCS)
        """
        self.folder_to_watch = folder_to_watch
        self.mandatory_files = [file for file in export_required_files if file not in [
            "Presentkort_sold.csv", "Presentkort_used.csv", "Följesedlar.csv"
        ]]
        self.optional_files = ["Presentkort_sold.csv", "Presentkort_used.csv", "Följesedlar.csv"]
        self.import_required_file = import_required_file
        print(f"Initialized FileRenameHandler instance: {id(self)}")

    def _all_mandatory_files_present(self):
        """Check if all mandatory export files exist (ignoring optional ones)."""
        current_files = set(os.listdir(self.folder_to_watch))
        print(f"Current files in folder: {current_files}")
        return all(req_file in current_files for req_file in self.mandatory_files)

    def _get_optional_files(self):
        """Check which optional files exist and return their paths."""
        current_files = set(os.listdir(self.folder_to_watch))
        return [
            os.path.join(self.folder_to_watch, file) 
            for file in self.optional_files if file in current_files
        ]

    def _is_import_file_present(self):
        """Check if the import file (PCS.ADM) exists."""
        return self.import_required_file in os.listdir(self.folder_to_watch)

    def _process_files(self):
        # Import scenario
        if self._is_import_file_present():
            file_path = os.path.join(self.folder_to_watch, self.import_required_file)
            print("Detected PCS file. Starting import action.")
            custom_import_action([file_path], self.folder_to_watch)

        # Export scenario
        elif self._all_mandatory_files_present():
            file_paths = [
                os.path.join(self.folder_to_watch, file)
                for file in self.mandatory_files
            ]

            # Include optional files if they exist
            optional_files = self._get_optional_files()
            if optional_files:
                file_paths.extend(optional_files)
                print(f"Including optional files: {optional_files}")

            print("Detected all required export files. Starting export action.")
            custom_export_action(file_paths, self.folder_to_watch)
        else:
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
    folder_to_watch = "C:/winbag_export/Input_Files_Here"

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
