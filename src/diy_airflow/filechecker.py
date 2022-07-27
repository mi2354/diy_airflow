import os

# from diy_airflow.data_model import Pipeline
from diy_airflow.scheduler import Scheduler
from diy_airflow.utils import get_files_from_dir, process_filepath


class Watcher:
    def __init__(self, directory_to_watch: str, scheduler: Scheduler):
        self.directory_to_watch = directory_to_watch
        self.scheduler = scheduler
        self.files = {}

    def monitor(self):
        dirfiles = get_files_from_dir()
        for filepath in dirfiles:
            modification_time = os.path.getmtime(filepath)
            if (
                filepath in self.files and modification_time > self.files[filepath]
            ) or filepath not in self.files:
                pipeline = process_filepath(filepath)
                self.scheduler.process_pipeline(pipeline)
                self.files[filepath] = modification_time
