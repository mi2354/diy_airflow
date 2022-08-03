from datetime import datetime
from queue import PriorityQueue
from typing import Optional

from croniter import croniter
from wasabi import msg

from diy_airflow.data_model import Pipeline
from diy_airflow.utils import run_pipeline


class Scheduler:
    def __init__(self) -> None:
        self.q = PriorityQueue(maxsize=100)
        self.queued_ids = []

    def add_pipeline(self, pipeline: Optional[Pipeline]) -> None:
        """
        Add pipeline to queue
        """
        if isinstance(pipeline, Pipeline):
            if pipeline.name not in self.queued_ids:
                self.q.put(pipeline)
                self.queued_ids.append(pipeline.name)
            else:
                new_queue = PriorityQueue(maxsize=100)
                while not self.q.empty():
                    queued_pipeline = self.q.get()
                    if queued_pipeline.name == pipeline.name:
                        new_queue.put(pipeline)
                    else:
                        new_queue.put(queued_pipeline)
                self.q = new_queue

    def run(self):
        """
        Check next pipeline to be executed in the queue and run it if possible
        """
        if not self.q.empty():
            pipeline: Pipeline = self.q.get()
            pipeline_start_date = pipeline.start_date
            if pipeline_start_date < datetime.now():
                msg.info(f"Starting pipeline {pipeline.name}")
                try:
                    run_pipeline(pipeline)
                except Exception as e:
                    msg.fail(f"Failed to run pipeline {pipeline.name}")
                    print(e)
                else:
                    msg.good(f"Pipeline {pipeline.name} run successfully!")
                iter = croniter(pipeline.schedule, pipeline_start_date)
                next_datetime = iter.get_next(datetime)
                # With this loop we avoid that a long task blocks our schedule
                while next_datetime < datetime.now():
                    next_datetime = iter.get_next(datetime)
                pipeline.start_date = next_datetime
            self.q.put(pipeline)
