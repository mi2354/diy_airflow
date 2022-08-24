import os

from diy_airflow.scheduler import Scheduler
from diy_airflow.utils import get_files_from_dir, get_pipeline_from_file


class Watcher:
    def __init__(self, directory_to_watch: str, scheduler: Scheduler):
        self.directory_to_watch = directory_to_watch
        self.scheduler = scheduler
        self.files = {}

    def monitor(self):
        """
        Check if there are changes in self.directory_to_watch and if so
        get the pipelines and adds them to the scheduler
        """
        dirfiles = get_files_from_dir(self.directory_to_watch)
        for filepath in dirfiles:
            modification_time = os.path.getmtime(filepath)
            if (
                filepath in self.files and modification_time > self.files[filepath]
            ) or filepath not in self.files:
                pipeline_candidate = get_pipeline_from_file(filepath)
                self.scheduler.add_pipeline(pipeline_candidate)
                self.files[filepath] = modification_time
