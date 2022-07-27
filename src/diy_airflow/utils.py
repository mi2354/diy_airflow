import sys
from importlib.util import module_from_spec, spec_from_file_location
from os import listdir
from os.path import isfile, join
from typing import List, Optional

from wasabi import msg

from diy_airflow.data_model import Pipeline, validate_pipeline


def get_files_from_dir(dirpath: str) -> List[str]:
    files = []
    for f in listdir(dirpath):
        if isfile(join(dirpath, f)) and f.endswith(".py"):
            files.append(join(dirpath, f))
    return files


def get_pipeline_from_file(filepath: str) -> Optional[Pipeline]:
    """
    Given a file.py, return the Pipeline instance that is inside, if there is any

    Args:
        filepath (str): a file path "example.py"

    Returns:
        Optional[Pipeline]: A Pipeline instance if there is any inside the
                            script in filepath, else None
    """
    if filepath.endswith(".py"):
        spec = spec_from_file_location("module.name", filepath)
        mod = module_from_spec(spec)
        sys.modules["module.name"] = mod
        try:
            spec.loader.exec_module(mod)
        except Exception as e:
            msg.fail(f"Error in module {filepath}, cannot load module")
        else:
            if hasattr(mod, "pipeline") and isinstance(mod.pipeline, Pipeline):
                try:
                    validate_pipeline(mod.pipeline)
                    msg.info(f"Pipeline {mod.pipeline.name} from {filepath} is valid!")
                except TypeError:
                    msg.fail(f"Pipeline in {filepath} not valid")
                else:
                    return mod.pipeline


def run_pipeline(pipeline: Pipeline):
    for task in pipeline.tasks:
        msg.info(f"Starting task {task.name}")
        task.python_callable()
