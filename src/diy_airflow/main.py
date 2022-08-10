import time

import click

from diy_airflow.filechecker import Watcher
from diy_airflow.scheduler import Scheduler
from diy_airflow.state_saver import StateSaver


def start_scheduler(path: str):
    state_saver = StateSaver()
    state_saver.start()
    scheduler = Scheduler(state_saver)
    watcher = Watcher(path, scheduler)
    while True:
        watcher.monitor()
        scheduler.run()
        time.sleep(5)


def start_worker():
    pass


@click.command()
@click.argument("service", type=str)
@click.option("-p", "--path", type=str, default="examples")
def main(service: str, path: str) -> None:
    if service == "scheduler":
        click.echo(f"Selected path: {path}")
        start_scheduler(path)
    elif service == "worker":
        start_worker()


if __name__ == "__main__":
    main()
