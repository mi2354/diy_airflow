import time

import click

from diy_airflow.filechecker import Watcher
from diy_airflow.scheduler import Scheduler
from diy_airflow.state_saver import StateSaver


@click.command()
@click.argument("path", type=str, default="examples")
def main(path):
    click.echo(f"Selected path: {path}")
    state_saver = StateSaver()
    state_saver.start()
    scheduler = Scheduler(state_saver)
    watcher = Watcher(path, scheduler)
    while True:
        watcher.monitor()
        scheduler.run()
        time.sleep(5)


if __name__ == "__main__":
    main()
