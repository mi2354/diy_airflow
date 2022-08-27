import json
import os
from dataclasses import asdict
from enum import Enum

from redis import Redis

from diy_airflow.data_model import Pipeline, SimpleTask, Status

REDIS_HOST = os.getenv("REDIS_HOST", default="localhost")


class StateSaver:
    def start(self):
        self.r = Redis(host=REDIS_HOST)

    def save_pipeline_run(self, pipeline: Pipeline):
        name = pipeline.name
        run = pipeline.start_date.strftime("%Y/%m/%d, %H:%M:%S")
        self.r.rpush(name, run)

    def add_to_pool_ready(self, s_task: SimpleTask):
        element = asdict(s_task)
        json_element = json.dumps(element)
        self.r.rpush("PoolReady", json_element)

    def get_from_pool_ready(self) -> SimpleTask:
        x = self.r.lpop("PoolReady")
        if x is not None:
            s_task = json.loads(x)
            return SimpleTask(**s_task)

    def save_status(self, name: str, status: Enum):
        self.r.set(name, status.value)

    def check_status(self, name: str) -> Status:
        x = self.r.get(name)
        if isinstance(x, bytes):
            return Status(int(x))
        elif x is None:
            return Status(x)
        else:
            raise ValueError("Something went wrong when reading the status")
