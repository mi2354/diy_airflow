import click

from .filechecker import Watcher
from .utils import get_files_from_dir, process_filepath


@click.command()
@click.argument('path', type=str)
def main(path):
    click.echo(f'Selected path: {path}')
    for element in get_files_from_dir(path):
        process_filepath(element)
    w = Watcher(path)
    w.run()

 
if __name__ == '__main__':
    main()
