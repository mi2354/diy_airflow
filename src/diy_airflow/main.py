import click

from diy_airflow.filechecker import Watcher
from diy_airflow.utils import get_files_from_dir, process_filepath


@click.command()
@click.argument('path', type=str, default="examples")
def main(path):
    click.echo(f'Selected path: {path}')
    for element in get_files_from_dir(path):
        process_filepath(element)
    w = Watcher(path)
    w.run()

 
if __name__ == '__main__':
    main()
