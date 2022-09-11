from dataclasses import dataclass
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, List, Optional, Tuple

import networkx as nx
from croniter import croniter


class Task:
    pipeline_id: str
    task_id: str
    successors: list

    def __init__(self, name: str, python_callable: Callable):
        """
        Basic unit of execution. Tasks are arranged in Pipelines.

        Args:
            name (str): An identification of the task
            python_callable (Callable): The python function that
                needs to be executed in this task
        """
        self.name = name
        self.python_callable = python_callable
        self.successors = []

    def set_downstream(self, task):
        """
        Set the successor task. Multiple successors are possible
        executing this method multiple times

        Args:
            task (Task): The successor task that needs to run
        """
        self.successors.append(task)


class Pipeline:
    G: nx.DiGraph
    id_: str
    filepath: str

    def __init__(
        self,
        name: str,
        schedule: str,
        task_list: List[Task],
        start_date: Optional[datetime] = datetime.now(),
    ) -> None:
        """
        Collection of tasks that need to run in a certain order. It's the equivalent of
        DAGs in Airflow.

        Args:
            name (str): Identification for the pipeline
            schedule (str): A valid cron string, like "* * * * *"
            task_list (List[Task]): A list of tasks that need to run,
                order doesn't matter
            start_date (Optional[datetime], optional): The date when
                the pipeline needs to run for the first time.
                Defaults to datetime.now().
        """
        self.name = name
        self.schedule = schedule
        self.task_list = task_list
        self.start_date = start_date

    def __eq__(self, other):
        return self.start_date == other.start_date

    def __lt__(self, other):
        return self.start_date < other.start_date

    def build_digraph(self):
        G = nx.DiGraph()
        for task in self.task_list:
            for successor in task.successors:
                G.add_edge(task.name, successor.name)
        check_no_cycles(G)
        self.G = G

    def build_ids(self):
        self.id_ = f"{self.name}-{self.start_date}"
        for task in self.task_list:
            task.task_id = f"{self.id_}-{task.name}"


@dataclass
class SimplePipeline:
    id: str
    filepath: str
    graph: nx.DiGraph


@dataclass
class SimpleTask:
    pipeline_id: str
    name: str
    filepath: str


class Status(Enum):
    WAITING = auto()
    RUNNING = auto()
    FINISHED_SUCCESS = auto()
    FINISHED_FAIL = auto()
    NOT_SCHEDULED = None


def check_no_cycles(G: nx.Graph):
    """
    Check wether a nx.Graph has cycles, and if it does, raise an exception

    Args:
        G (nx.Graph): Any graph

    Raises:
        nx.HasACycle: When the graph has a cycle

    Returns:
        None: A python None
    """
    try:
        nx.find_cycle(G)
    except nx.NetworkXNoCycle:
        return None
    else:
        raise nx.HasACycle("Task has cycles!")


def validate_pipeline(pipeline: Any):
    """
    Given any python object, check if it's a diy_airflow.data_model.Pipeline
    If it's not, an Exception will be raised.

    Args:
        pipeline (Any): Any object that we expect is a pipeline
    """
    if not isinstance(pipeline, Pipeline):
        raise TypeError("pipeline provided is not an instance of Pipeline")
    if not isinstance(pipeline.name, str):
        raise TypeError("Pipeline name not a string")
    if not isinstance(pipeline.schedule, str):
        raise TypeError("Pipeline schedule is not a string")
    if not croniter.is_valid(pipeline.schedule):
        raise ValueError("Pipeline schedule is not a valid cron")
    if not isinstance(pipeline.start_date, datetime):
        raise TypeError("Pipeline start_date is not a datetime")
    if not isinstance(pipeline.task_list, List):
        raise TypeError("Pipeline tasks is not an instance of List")
    tasks_names = []
    for element in pipeline.task_list:
        if not isinstance(element, Task):
            raise TypeError("Pipeline tasks elements are not a Task instance")
        tasks_names.append(element.name)
    if len(set(tasks_names)) != len(tasks_names):
        raise ValueError("Task names should be unique!")
