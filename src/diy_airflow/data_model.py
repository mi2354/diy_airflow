from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, List

from croniter import croniter
import networkx as nx


class Task:
    def __init__(self, name: str, python_callable: Callable):
        self.name = name
        self.python_callable = python_callable
        self.successors = []

    def set_downstream(self, task):
        self.successors.append(task)


@dataclass
class Pipeline:
    name: str
    schedule: str
    tasks: List[Task]
    start_date: datetime = datetime.now()

    def __eq__(self, other):
        return self.start_date == other.start_date

    def __lt__(self, other):
        return self.start_date < other.start_date
    
    def build_digraph(self):
        G = nx.DiGraph()
        for task in self.tasks:
            for successor in task.successors:
                G.add_edge(task, successor)
        check_no_cycles(G)
        self.G = G


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
    if not isinstance(pipeline.tasks, List):
        raise TypeError("Pipeline tasks is not an instance of List")
    for element in pipeline.tasks:
        if not isinstance(element, Task):
            raise TypeError("Pipeline tasks elements are not a Task instance")

