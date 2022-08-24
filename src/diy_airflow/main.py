import sys
import time

import click

from diy_airflow.filechecker import Watcher
from diy_airflow.scheduler import Scheduler
from diy_airflow.state_saver import StateSaver
from diy_airflow.worker import Worker

sys.stdout.flush


def start_scheduler(path: str):
    print("Booting up scheduler", flush=True)
    state_saver = StateSaver()
    state_saver.start()
    state_saver.r.flushdb()  # Problem comes from repeated keys in redis.
    # Check https://stdworkflow.com/383/redis-wrongtype-operation-against-a-key-holding-the-wrong-kind-of-value
    scheduler = Scheduler(state_saver)
    watcher = Watcher(path, scheduler)
    while True:
        watcher.monitor()
        scheduler.run()
        time.sleep(5)


def start_worker():
    print("Booting up worker", flush=True)
    state_saver = StateSaver()
    state_saver.start()
    worker = Worker(state_saver)
    while True:
        worker.run()
        time.sleep(5)


@click.command()
@click.argument("service", type=str)
@click.option("-p", "--path", type=str, default="pipelines")
def main(service: str, path: str) -> None:
    if service == "scheduler":
        click.echo(f"Selected path: {path}")
        start_scheduler(path)
    elif service == "worker":
        start_worker()


if __name__ == "__main__":
    main()
