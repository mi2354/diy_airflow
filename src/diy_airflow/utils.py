from modulefinder import ModuleFinder
from os import listdir
from os.path import isfile, join
from typing import List

from data_model import Pipeline


def files_files_from_dir(dirpath: str) -> List[str]:
    return [f for f in listdir(dirpath) if isfile(join(dirpath, f))]


def get_pipeline_from_file(filepath: str) -> Pipeline:
    finder = ModuleFinder()
    finder.run_script(filepath)
    for name, mod in finder.modules.items():
        if isinstance(mod, Pipeline):
            print(f"Pipeline {mod.name} in file {filepath}")
            return mod