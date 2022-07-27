from diy_airflow.data_model import Pipeline, validate_pipeline
from queue import PriorityQueue


class Scheduler:
    def __init__(self) -> None:
        self.q = PriorityQueue(maxsize=100)

    def process_pipeline(self, pipeline: Pipeline) -> None:
        validate_pipeline(pipeline)
        print(f"Pipeline {pipeline.name} valid")
        self.q.put(pipeline)
