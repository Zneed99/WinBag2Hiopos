import os
import shutil
import time
import pytz
import traceback
import re
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from export import export_action
from import_ import import_action

def move_files_to_old_folder(file_paths, folder_to_watch):
    old_folder_path = os.path.join("C:/winbag_export", "Old Files")
    if not os.path.exists(old_folder_path):
        os.makedirs(old_folder_path)
        print(f"Created 'Old Files' folder at {old_folder_path}")

    stockholm_tz = pytz.timezone("Europe/Stockholm")
    current_time = datetime.now(stockholm_tz).strftime("%Y%m%d-%H-%M-%S")

    for file_path in file_paths:
        file_name = os.path.basename(file_path)
        file_name_parts = os.path.splitext(file_name)
        new_file_name = f"{file_name_parts[0]}_{current_time}_old{file_name_parts[1]}"

        old_file_path = os.path.join(old_folder_path, new_file_name)
        shutil.move(file_path, old_file_path)
        print(f"Moved {file_name} to 'Old Files' as {new_file_name}.")

def extract_date_from_filename(filename):
    match = re.search(r"Försäljning_(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})", filename)
    if match:
        return match.group(1)
    return None

def custom_export_action(file_paths, folder_to_watch):
    try:
        print("Performing export...")
        export_action(file_paths)
        move_files_to_old_folder(file_paths, folder_to_watch)
    except Exception as e:
        tb = traceback.format_exc()
        print(f"An error occurred during export_action:\n{tb}")

def custom_import_action(file_path, folder_to_watch):
    try:
        print("Performing import...")
        import_action(file_path)
        move_files_to_old_folder(file_path, folder_to_watch)
    except Exception as e:
        tb = traceback.format_exc()
        print(f"An error occurred during import_action: {tb}")

class FileRenameHandler(FileSystemEventHandler):
    def __init__(
        self, export_folder, import_folder, export_required_keywords, import_required_keyword
    ):
        self.export_folder = export_folder
        self.import_folder = import_folder
        self.mandatory_keywords = [
            kw
            for kw in export_required_keywords
            if kw not in ["Presentkort_sold", "Presentkort_used", "Följesedlar"]
        ]
        self.optional_keywords = ["Presentkort_sold", "Presentkort_used", "Följesedlar"]
        self.import_required_keyword = import_required_keyword
        print(f"Initialized FileRenameHandler instance: {id(self)}")

    def _find_files_with_keywords(self, folder, keywords):
        current_files = os.listdir(folder)
        matching_files = []
        for file in current_files:
            if any(keyword in file for keyword in keywords):
                matching_files.append(os.path.join(folder, file))
        return matching_files

    def _all_mandatory_files_present(self):
        matching_files = self._find_files_with_keywords(self.export_folder, self.mandatory_keywords)
        print(f"Matching mandatory files: {matching_files}")
        return len(matching_files) >= len(self.mandatory_keywords)

    def _get_optional_files(self):
        return self._find_files_with_keywords(self.export_folder, self.optional_keywords)

    def _is_import_file_present(self):
        return any(
            self.import_required_keyword in file
            for file in os.listdir(self.import_folder)
        )

    def _process_files(self):
        if self._is_import_file_present():
            import_files = self._find_files_with_keywords(self.import_folder, [self.import_required_keyword])
            for file_path in import_files:
                print(f"Detected PCS file: {file_path}. Starting import action.")
                custom_import_action([file_path], self.import_folder)

        elif self._all_mandatory_files_present():
            mandatory_files = self._find_files_with_keywords(self.export_folder, self.mandatory_keywords)
            optional_files = self._get_optional_files()
            file_paths = mandatory_files + optional_files

            sales_date = None
            for file_path in mandatory_files:
                if "Försäljning" in file_path:
                    sales_date = extract_date_from_filename(os.path.basename(file_path))
                    break

            if optional_files:
                print(f"Including optional files: {optional_files}")

            print("Detected all required export files. Starting export action.")
            custom_export_action(file_paths, self.export_folder)
        else:
            print("Waiting for PCS or all required export files...")

    def on_created(self, event):
        if not event.is_directory:
            print(f"File added: {event.src_path}")
            self._process_files()

def monitor_folders(export_folder, import_folder, export_required_keywords, import_required_keyword):
    event_handler = FileRenameHandler(
        export_folder=export_folder,
        import_folder=import_folder,
        export_required_keywords=export_required_keywords,
        import_required_keyword=import_required_keyword,
    )
    observer = Observer()
    observer.schedule(event_handler, export_folder, recursive=False)
    observer.schedule(event_handler, import_folder, recursive=False)
    observer.start()
    print(f"Monitoring export folder: {export_folder}")
    print(f"Monitoring import folder: {import_folder}")

    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        observer.stop()
        print("Stopped monitoring.")
    observer.join()

if __name__ == "__main__":
    export_folder = "C:/winbag_export/Input_Files_Here"
    import_folder = "C:/winbag_export"
    export_required_keywords = ["Försäljning", "Betalsätt", "Följesedlar", "Moms"]
    import_required_keyword = "PCS.ADM"

    if os.path.exists(export_folder) and os.path.exists(import_folder):
        monitor_folders(export_folder, import_folder, export_required_keywords, import_required_keyword)
    else:
        print("One or both specified folders do not exist. Creating missing folders...")
        try:
            os.makedirs(export_folder, exist_ok=True)
            os.makedirs(import_folder, exist_ok=True)
            monitor_folders(export_folder, import_folder, export_required_keywords, import_required_keyword)
        except Exception as e:
            print(f"Failed to create folders. Error: {e}")
