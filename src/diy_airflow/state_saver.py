from enum import Enum
from redis import Redis
from diy_airflow.data_model import Pipeline, Status, Task
import os
import json

REDIS_HOST = os.getenv("REDIS_HOST", default="localhost")


class StateSaver:
    def start(self):
        self.r = Redis(host=REDIS_HOST)
        # self.r.flushdb() # Ask how to deal with this

    def save_pipeline_run(self, pipeline: Pipeline):
        name = pipeline.name
        run = pipeline.start_date.strftime("%Y/%m/%d, %H:%M:%S")
        self.r.rpush(name, run)

    def add_to_pool_ready(self, element: dict):
        json_element = json.dumps(element)
        self.r.rpush("PoolReady", json_element)
        print(f"Save PoolReady: {json_element}", flush=True)

    def get_from_pool_ready(self):
        x = self.r.lpop("PoolReady")
        # Read this: https://stackoverflow.com/questions/37953019/wrongtype-operation-against-a-key-holding-the-wrong-kind-of-value-php
        if x is not None:
            s_task = json.loads(x)
            return s_task

    def save_status(self, name: str, status: Enum):
        print(f"Save state: {[name, status.value]}", flush=True)
        self.r.set(name, status.value)

    def check_status(self, name: str):
        x = self.r.get(name)
        if isinstance(x, bytes):
            return Status(int(x))
        elif x is None:
            return Status(x)
        else:
            raise ValueError("Something went wrong when reading the status")
