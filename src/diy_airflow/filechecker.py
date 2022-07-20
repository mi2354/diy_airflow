import time
from diy_airflow.data_model import Pipeline
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from diy_airflow.utils import process_filepath

# Shamelessly stolen from Ivan

class Handler(FileSystemEventHandler):
    @staticmethod
    def on_any_event(event):
        if event.is_directory or event.src_path.endswith('pyc'):
            return None

        elif event.event_type == "created":
            # Take any action here when a file is first created.
            print(f"Received created event - {event.src_path}.")

        elif event.event_type == "modified":
            # Taken any action here when a file is modified.
            print(f"Received modified event - {event.src_path}.")

        process_filepath(event.src_path)


class Watcher:
    def __init__(self, directory_to_watch):
        self.observer = Observer()
        self.directory_to_watch = directory_to_watch

    def run(self):
        event_handler = Handler()
        self.observer.schedule(event_handler, self.directory_to_watch, recursive=True)
        self.observer.start()
        print(f"Waiting for changes in {self.directory_to_watch}")
        try:
            while True:
                time.sleep(5)
        except:
            self.observer.stop()
            print("Stopped monitoring!")

        self.observer.join()
