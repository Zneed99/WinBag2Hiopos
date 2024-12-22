import os
import time
import win32serviceutil
import win32service
import win32event
import servicemanager
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

class FolderMonitorService(win32serviceutil.ServiceFramework):
    _svc_name_ = "FolderMonitorService"
    _svc_display_name_ = "Folder Monitor Service"
    _svc_description_ = "Monitors a folder and renames files using Python and Watchdog."

    def __init__(self, args):
        super().__init__(args)
        self.stop_event = win32event.CreateEvent(None, 0, 0, None)
        self.running = True

    def SvcStop(self):
        # Signal the service is stopping
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.stop_event)
        self.running = False

    def SvcDoRun(self):
        servicemanager.LogInfoMsg("Hejhopp")
        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE,
            servicemanager.PYS_SERVICE_STARTED,
            (self._svc_name_, "")
        )
        self.ReportServiceStatus(win32service.SERVICE_RUNNING)
        servicemanager.LogInfoMsg("Service is running... Starting main logic.")
        try:
            self.main()
        except Exception as e:
            servicemanager.LogErrorMsg(f"Unhandled exception: {e}")
            raise

    def main(self):
        servicemanager.LogInfoMsg("Hejhopp")
        folder_to_watch = "C:/Users/holme/OneDrive/Skrivbord/Install-Testing-System-Service"
        new_name_format = "Testing very nice"

        servicemanager.LogInfoMsg("Service starting...")

        if not os.path.exists(folder_to_watch):
            servicemanager.LogErrorMsg(f"Folder does not exist: {folder_to_watch}")
            self.running = False
            return

        # Set up the Watchdog observer
        event_handler = FileRenameHandler(folder_to_watch, new_name_format)
        observer = Observer()
        observer.schedule(event_handler, folder_to_watch, recursive=False)
        observer.start()
        servicemanager.LogInfoMsg(f"Monitoring folder: {folder_to_watch}")

        try:
            while self.running:
                time.sleep(1)  # Keep the service running
        except Exception as e:
            servicemanager.LogErrorMsg(f"Service encountered an error: {e}")
        finally:
            observer.stop()
            observer.join()
            servicemanager.LogInfoMsg("Service stopped monitoring.")

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "standalone":
        # Standalone mode: Run folder monitoring directly
        folder_to_watch = "C:/Users/holme/OneDrive/Skrivbord/Install-Testing-System-Service"
        new_name_format = "Testing very nice"
        

        if os.path.exists(folder_to_watch):
            print(f"Running in standalone mode. Monitoring: {folder_to_watch}")
            try:
                event_handler = FileRenameHandler(folder_to_watch, new_name_format)
                observer = Observer()
                observer.schedule(event_handler, folder_to_watch, recursive=False)
                observer.start()

                try:
                    while True:
                        time.sleep(1)  # Keep the script running
                except KeyboardInterrupt:
                    observer.stop()
                    print("Stopped monitoring.")
                observer.join()
            except Exception as e:
                print(f"An error occurred: {e}")
        else:
            print("The specified folder does not exist.")
    else:
        # Service mode: Handle service commands
        win32serviceutil.HandleCommandLine(FolderMonitorService)

