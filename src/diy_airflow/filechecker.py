import time
from diy_airflow.data_model import Pipeline
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from diy_airflow.utils import process_filepath
from diy_airflow.scheduler import Scheduler

# Shamelessly stolen from Ivan


class Handler(FileSystemEventHandler):
    def __init__(self, scheduler: Scheduler) -> None:
        self.scheduler = scheduler
        super().__init__()

    def on_any_event(self, event):
        if event.is_directory or event.src_path.endswith("pyc"):
            return None

        elif event.event_type == "created":
            # Take any action here when a file is first created.
            print(f"Received created event - {event.src_path}.")

        elif event.event_type == "modified":
            # Taken any action here when a file is modified.
            print(f"Received modified event - {event.src_path}.")

        pipeline = process_filepath(event.src_path)
        self.scheduler.process_pipeline(pipeline)


class Watcher:
    def __init__(self, directory_to_watch):
        self.observer = Observer()
        self.directory_to_watch = directory_to_watch

    def run(self, scheduler):
        event_handler = Handler(scheduler=scheduler)
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
