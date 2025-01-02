import os
import shutil
import time
import pytz
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from export import export_action


class FileRenameHandler(FileSystemEventHandler):
    def __init__(self, folder_to_watch, required_files, action_function):
        self.folder_to_watch = folder_to_watch
        self.required_files = required_files
        self.action_function = action_function
        self.old_folder_path = os.path.join(
            folder_to_watch, "Old Files"
        )  # Path to "Old Files" folder
        print(f"Initialized FileRenameHandler instance: {id(self)}")

    def _all_files_present(self):
        """Check if all required files are present."""
        current_files = set(os.listdir(self.folder_to_watch))
        return all(req_file in current_files for req_file in self.required_files)

    def _missing_files(self):
        """Return a list of missing files."""
        current_files = set(os.listdir(self.folder_to_watch))
        missing_files = [
            file for file in self.required_files if file not in current_files
        ]
        return missing_files

    def _process_files(self):
        """Trigger the action if all required files are present."""

        missing_files = self._missing_files()

        if not missing_files:
            print("All required files are present. Processing...")
            file_paths = [
                os.path.join(self.folder_to_watch, file) for file in self.required_files
            ]
            # Call the custom action
            self.action_function(file_paths, self)
        else:
            print(
                "Waiting for required files: ",
                ", ".join(missing_files) if missing_files else "None",
            )

    def on_created(self, event):
        if not event.is_directory:
            file_path = event.src_path
            print(f"File added: {file_path}")

            self._process_files()


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
        # Get the file name and add the current timestamp and 'old' to the file name
        file_name = os.path.basename(file_path)
        file_name_parts = os.path.splitext(file_name)
        new_file_name = f"{file_name_parts[0]}_{current_time}_old{file_name_parts[1]}"

        old_file_path = os.path.join(old_folder_path, new_file_name)
        shutil.move(file_path, old_file_path)
        print(f"Moved {file_name} to 'Old Files' as {new_file_name}.")


def custom_action(file_paths, event_handler):
    # Perform the export action
    export_action(file_paths)

    # Move the files to the 'Old Files' folder after export
    move_files_to_old_folder(file_paths, folder_to_watch)


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
    folder_to_watch = "C:/Users/FelixHolmesten/InstallSystemService"
    required_files = ["Försäljning.csv", "Betalsätt.csv", "Följesedlar.csv"]

    if os.path.exists(folder_to_watch):
        monitor_folder(folder_to_watch, required_files)
    else:
        print("The specified folder does not exist.")
