import click

from diy_airflow.filechecker import Watcher
from diy_airflow.scheduler import Scheduler
from diy_airflow.utils import get_files_from_dir, process_filepath


@click.command()
@click.argument("path", type=str, default="examples")
def main(path):
    click.echo(f"Selected path: {path}")
    scheduler = Scheduler()
    for element in get_files_from_dir(path):
        pipeline = process_filepath(element)
        scheduler.process_pipeline(pipeline)
    w = Watcher(path)
    w.run(scheduler)


if __name__ == "__main__":
    main()
