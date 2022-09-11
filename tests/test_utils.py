import unittest
from pathlib import Path

from diy_airflow.data_model import Pipeline
from diy_airflow.utils import get_pipeline_from_file, get_py_files_from_dir

test_pipelines_path = f"{Path(__file__).parent.absolute()}/test_pipelines/"


class TestUtils(unittest.TestCase):
    def test_get_pipeline_from_file(self):
        # Doesn't raise an error
        output1 = get_pipeline_from_file("folder_that_doesn't_exist")
        assert output1 is None

        output2 = get_pipeline_from_file(f"{test_pipelines_path}wrong_pipeline.py")
        assert output2 is None

        output3 = get_pipeline_from_file(f"{test_pipelines_path}good_pipeline.py")
        assert isinstance(output3, Pipeline)

    def test_get_py_files_from_dir(self):
        files = get_py_files_from_dir(test_pipelines_path)
        assert f"{test_pipelines_path}good_pipeline.py" in files
        assert f"{test_pipelines_path}wrong_pipeline.py" in files
