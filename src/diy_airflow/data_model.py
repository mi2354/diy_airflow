from dataclasses import dataclass
from datetime import datetime
from typing import Callable, List, Optional, Tuple
from enum import Enum, auto

from croniter import croniter
import networkx as nx


class Task:
    pipeline_id: str
    pipeline_start_date: datetime
    relative_order: int
    task_id: str
    predecessors: list
    successors: list

    def __init__(self, name: str, python_callable: Callable):
        self.name = name
        self.python_callable = python_callable
        self.successors = []
        self.predecessors = []

    def set_downstream(self, task):
        self.successors.append(task)
        task._set_predecessor(self.name)

    def _set_predecessor(self, pred_name):
        self.predecessors.append(pred_name)


class Pipeline:
    G: nx.DiGraph
    id_: str
    sorted_tasks: List[Task]
    filepath: str

    def __init__(
        self,
        name: str,
        schedule: str,
        task_list: List[Task],
        start_date: Optional[datetime] = datetime.now(),
    ) -> None:
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

    # def build_sorted_tasks(self):
    #     self._build_digraph()
    #     self.sorted_tasks = list(nx.topological_sort(self.G))

    def build_ids(self):
        self.id_ = f"{self.name}-{self.start_date}"
        for task in self.task_list:
            task.task_id = f"{self.id_}-{task.name}"

    # def get_sorted_task_ids(self) -> List[str]:
    #     # TODO: This is extremely inefficient... Change it when everything works
    #     sorted_task_ids = []
    #     for sorted_task in self.sorted_tasks:
    #         for task in self.task_list:
    #             if sorted_task.name == task.name:
    #                 sorted_task_ids.append(task.task_id)
    #     return sorted_task_ids


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
    FINISHED = auto()
    NOT_SCHEDULED = None


def check_no_cycles(G: nx.Graph):
    try:
        nx.find_cycle(G)
    except nx.NetworkXNoCycle:
        return None
    else:
        raise nx.HasACycle("Task has cycles!")


def validate_pipeline(pipeline: Pipeline):
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
