from datetime import date, datetime
from queue import PriorityQueue

from croniter import croniter
from wasabi import msg

from diy_airflow.data_model import Pipeline
from typing import Optional
from diy_airflow.utils import run_pipeline


class Scheduler:
    def __init__(self) -> None:
        self.q = PriorityQueue(maxsize=100)

    def add_pipeline(self, pipeline: Optional[Pipeline]) -> None:
        if isinstance(pipeline, Pipeline):
            self.q.put(pipeline)

    def run(self):
        if not self.q.empty():
            pipeline: Pipeline = self.q.get()
            pieline_start_date = pipeline.start_date
            if pieline_start_date < datetime.now():
                msg.info(f"Starting pipeline {pipeline.name}")
                try:
                    run_pipeline(pipeline)
                except Exception as e:
                    msg.fail(f"Failed to run pipeline {pipeline.name}")
                    print(e)
                else:
                    msg.good(f"Pipeline {pipeline.name} run successfully!")
                iter = croniter(pipeline.schedule, pieline_start_date)
                next_datetime = iter.get_next(datetime)
                # With this loop we avoid that a long task blocks our schedule
                while next_datetime < datetime.now():
                    next_datetime = iter.get_next(datetime)
                pipeline.start_date = next_datetime
            self.q.put(pipeline)
