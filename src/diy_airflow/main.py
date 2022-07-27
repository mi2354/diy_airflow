import time

import click

from diy_airflow.filechecker import Watcher
from diy_airflow.scheduler import Scheduler


@click.command()
@click.argument("path", type=str, default="examples")
def main(path):
    click.echo(f"Selected path: {path}")
    scheduler = Scheduler()
    watcher = Watcher(path, scheduler)
    while True:
        watcher.monitor()
        scheduler.run()
        time.sleep(5)


if __name__ == "__main__":
    main()
