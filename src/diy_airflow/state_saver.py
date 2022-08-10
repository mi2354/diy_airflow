from redis import Redis
from diy_airflow.data_model import Pipeline, Task
import os

REDIS_HOST = os.getenv("REDIS_HOST", default="localhost")

class StateSaver:
    def start(self):
        self.r = Redis(host=REDIS_HOST)

    def save_pipeline_run(self, pipeline: Pipeline):
        name = pipeline.name
        run = pipeline.start_date.strftime("%Y/%m/%d, %H:%M:%S")
        self.r.rpush(name, run)
