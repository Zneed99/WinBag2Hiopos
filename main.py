import os
import shutil
import time
import pytz
import re
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from export import export_action
from import_ import import_action
from logger_setup import get_logger

logger = get_logger(__name__)

def move_files_to_old_folder(file_paths, folder_to_watch):
    old_folder_path = os.path.join("C:/winbag_export", "Old Files")
    if not os.path.exists(old_folder_path):
        os.makedirs(old_folder_path)
        logger.info(f"Created 'Old Files' folder at {old_folder_path}")

    stockholm_tz = pytz.timezone("Europe/Stockholm")
    current_time = datetime.now(stockholm_tz).strftime("%Y%m%d-%H-%M-%S")

    for file_path in file_paths:
        file_name = os.path.basename(file_path)
        file_name_parts = os.path.splitext(file_name)
        new_file_name = f"{file_name_parts[0]}_{current_time}_old{file_name_parts[1]}"

        old_file_path = os.path.join(old_folder_path, new_file_name)
        shutil.move(file_path, old_file_path)
        logger.info(f"Moved {file_name} to 'Old Files' as {new_file_name}.")

def wait_for_file_stable(file_path, poll_interval=0.5, stable_checks=3, timeout=60):
    """
    Waits until file_path's size stops changing, so we don't start reading a
    file while the program that created it (WinBag) is still writing it.
    watchdog's on_created fires the moment the file appears, not when the
    writer closes it, so reading immediately can silently pick up a partial
    file - no exception, just fewer rows than expected. Returns True once the
    size has held steady across `stable_checks` polls, False on timeout.
    """
    start = time.time()
    last_size = -1
    stable_count = 0
    while time.time() - start < timeout:
        try:
            current_size = os.path.getsize(file_path)
        except OSError:
            current_size = -1

        if current_size == last_size and current_size >= 0:
            stable_count += 1
            if stable_count >= stable_checks:
                return True
        else:
            stable_count = 0

        last_size = current_size
        time.sleep(poll_interval)

    logger.warning(f"Timed out waiting for {file_path} to finish being written; proceeding anyway.")
    return False

def extract_date_from_filename(filename):
    match = re.search(r"Försäljning_(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})", filename)
    if match:
        return match.group(1)
    return None

def archive_pcs_file(file_path):
    """
    Moves the raw pcs.adm file out of the watched import folder into its own
    archive folder BEFORE processing starts. Doing this first (rather than
    after import_action runs) closes the window where a leftover pcs.adm in
    the root folder gets picked up and re-imported by an unrelated file event.
    """
    archive_folder = os.path.join("C:/winbag_export", "PCS_Archive")
    if not os.path.exists(archive_folder):
        os.makedirs(archive_folder)
        logger.info(f"Created 'PCS_Archive' folder at {archive_folder}")

    stockholm_tz = pytz.timezone("Europe/Stockholm")
    current_time = datetime.now(stockholm_tz).strftime("%Y%m%d-%H-%M-%S")

    file_name = os.path.basename(file_path)
    file_name_parts = os.path.splitext(file_name)
    new_file_name = f"{file_name_parts[0]}_{current_time}{file_name_parts[1]}"
    new_file_path = os.path.join(archive_folder, new_file_name)

    shutil.move(file_path, new_file_path)
    logger.info(f"Archived {file_name} to 'PCS_Archive' as {new_file_name}.")
    return new_file_path

def custom_export_action(file_paths, folder_to_watch):
    try:
        logger.info("Performing export...")
        export_action(file_paths)
        move_files_to_old_folder(file_paths, folder_to_watch)
    except Exception:
        logger.exception("An error occurred during export_action")

def custom_import_action(file_paths):
    try:
        logger.info("Performing import...")
        archived_path = archive_pcs_file(file_paths[0])
        import_action([archived_path])
    except Exception:
        logger.exception("An error occurred during import_action")

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
        logger.info(f"Initialized FileRenameHandler instance: {id(self)}")

    def _find_files_with_keywords(self, folder, keywords):
        current_files = os.listdir(folder)
        matching_files = []
        for file in current_files:
            if any(keyword in file for keyword in keywords):
                matching_files.append(os.path.join(folder, file))
        return matching_files

    def _all_mandatory_files_present(self):
        matching_files = self._find_files_with_keywords(self.export_folder, self.mandatory_keywords)
        return len(matching_files) >= len(self.mandatory_keywords)

    def _get_optional_files(self):
        return self._find_files_with_keywords(self.export_folder, self.optional_keywords)

    def _is_import_file_present(self):
        # Case-insensitive: WinBag's own export can write the filename in any
        # case (e.g. "pcs.adm"), and Windows filenames aren't case-sensitive.
        return any(
            self.import_required_keyword.lower() in file.lower()
            for file in os.listdir(self.import_folder)
        )

    def _find_import_files(self):
        current_files = os.listdir(self.import_folder)
        keyword_lower = self.import_required_keyword.lower()
        return [
            os.path.join(self.import_folder, file)
            for file in current_files
            if keyword_lower in file.lower()
        ]

    def _process_files(self):
        if self._is_import_file_present():
            import_files = self._find_import_files()
            for file_path in import_files:
                logger.info(f"Detected PCS file: {file_path}. Waiting for it to finish being written.")
                wait_for_file_stable(file_path)
                logger.info(f"Starting import action for {file_path}.")
                custom_import_action([file_path])

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
                logger.info(f"Including optional files: {optional_files}")

            logger.info("Detected all required export files. Waiting for them to finish being written.")
            for file_path in file_paths:
                wait_for_file_stable(file_path)

            logger.info("Starting export action.")
            custom_export_action(file_paths, self.export_folder)
        else:
            logger.debug(
                f"Waiting for PCS or all required export files... "
                f"(export folder: {os.listdir(self.export_folder)}, "
                f"import folder: {os.listdir(self.import_folder)})"
            )

    def on_created(self, event):
        if not event.is_directory:
            logger.debug(f"File created: {event.src_path}")
            self._process_files()

    def on_moved(self, event):
        # Some tools write to a temp name and rename/move it into place,
        # which watchdog reports as a move rather than a create.
        if not event.is_directory:
            logger.debug(f"File moved into place: {event.dest_path}")
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
    logger.info(f"Monitoring export folder: {export_folder}")
    logger.info(f"Monitoring import folder: {import_folder}")

    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        observer.stop()
        logger.info("Stopped monitoring.")
    observer.join()

if __name__ == "__main__":
    export_folder = "C:/winbag_export/Input_Files_Here"
    import_folder = "C:/winbag_export"
    export_required_keywords = ["Försäljning", "Betalsätt", "Följesedlar", "Moms"]
    import_required_keyword = "PCS.ADM"

    logger.info("WinBag2Hiopos starting up.")

    if os.path.exists(export_folder) and os.path.exists(import_folder):
        monitor_folders(export_folder, import_folder, export_required_keywords, import_required_keyword)
    else:
        logger.warning("One or both specified folders do not exist. Creating missing folders...")
        try:
            os.makedirs(export_folder, exist_ok=True)
            os.makedirs(import_folder, exist_ok=True)
            monitor_folders(export_folder, import_folder, export_required_keywords, import_required_keyword)
        except Exception:
            logger.exception("Failed to create folders.")
