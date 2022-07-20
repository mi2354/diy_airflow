from modulefinder import ModuleFinder
from os import listdir
from os.path import isfile, join
from typing import List, Optional

from .data_model import Pipeline


def get_files_from_dir(dirpath: str) -> List[str]:
    return [join(dirpath, f) for f in listdir(dirpath) if isfile(join(dirpath, f))]


def get_pipeline_from_file(filepath: str) -> Optional[Pipeline]:
    if filepath.endswith('.py'):
        finder = ModuleFinder()
        finder.run_script(filepath)
        for name, mod in finder.modules.items():
            if isinstance(mod, Pipeline):
                print(f"Pipeline {mod.name} in file {filepath}")
                return mod
        print(f'No pipeline found in {filepath}')



def send_to_queue(pipeline: Pipeline) -> None:
    print('Pipeline sent to queue')
    pass


def process_filepath(filepath: str) -> None:
        pipeline = get_pipeline_from_file(filepath)
        if pipeline is not None:
            send_to_queue(pipeline)