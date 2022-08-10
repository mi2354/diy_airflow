from redis import Redis
from diy_airflow.data_model import Pipeline, Task


class StateSaver:
    def start(self):
        self.r = Redis('redis')

    def save_pipeline_run(self, pipeline: Pipeline):
        name = pipeline.name
        run = pipeline.start_date.strftime("%Y/%m/%d, %H:%M:%S")
        self.r.rpush(name, run)
