import sys
from importlib.util import module_from_spec, spec_from_file_location
from os import listdir
from os.path import isfile, join
from typing import List, Optional

from diy_airflow.data_model import Pipeline, Task, validate_pipeline


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
            print(f"Error in module {filepath}, cannot load module", flush=True)
        else:
            if hasattr(mod, "pipeline") and isinstance(mod.pipeline, Pipeline):
                try:
                    validate_pipeline(mod.pipeline)
                    mod.pipeline.build_digraph()
                except Exception as e:
                    print(f"Pipeline in {filepath} not valid", flush=True)
                    print(f"Error in {filepath}: {e}", flush=True)
                else:
                    mod.pipeline.filepath = filepath
                    return mod.pipeline
