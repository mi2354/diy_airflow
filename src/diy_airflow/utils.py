import sys
from importlib.util import module_from_spec, spec_from_file_location
from os import listdir
from os.path import isfile, join
from typing import List, Optional

from diy_airflow.data_model import Pipeline


def get_files_from_dir(dirpath: str) -> List[str]:
    return [join(dirpath, f) for f in listdir(dirpath) if isfile(join(dirpath, f))]


def get_pipeline_from_file(filepath: str) -> Optional[Pipeline]:
    """
    Given a file.py, return the Pipeline instance that is inside, if there is any

    Args:
        filepath (str): a file path "example.py"

    Returns:
        Optional[Pipeline]: A Pipeline instance if there is any inside the
                            script in filepath, else None
    """
    if filepath.endswith('.py'):
        spec = spec_from_file_location("module.name", filepath)
        mod = module_from_spec(spec)
        sys.modules["module.name"] = mod
        spec.loader.exec_module(mod)
        if hasattr(mod, "Pipeline") and isinstance(mod.Pipeline, Pipeline):
            print(f"Found pipeline in {filepath}")
            return mod.Pipeline


def send_to_queue(pipeline: Pipeline) -> None:
    print('Pipeline sent to queue')
    pass


def process_filepath(filepath: str) -> None:
        pipeline = get_pipeline_from_file(filepath)
        if pipeline is not None:
            send_to_queue(pipeline)
