from diy_airflow.data_model import Pipeline, validate_pipeline
from queue import PriorityQueue
from datetime import date, datetime


class Scheduler:
    def __init__(self) -> None:
        self.q = PriorityQueue(maxsize=100)

    def process_pipeline(self, pipeline: Pipeline) -> None:
        validate_pipeline(pipeline)
        print(f"Pipeline {pipeline.name} valid")
        self.q.put(pipeline)

    def run(self):
        if not self.q.empty():
            pipeline = self.q.get()
            if pipeline.start_date > datetime.now():
                print(f"Starting pipeline {pipeline.name}")
                ### start pipeline
            else:
                self.q.put(pipeline)
