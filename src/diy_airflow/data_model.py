from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, List

from croniter import croniter
import networkx as nx


@dataclass
class Task:
    name: str
    python_callable: Callable
    predecessors : list = field(default_factory=list)

    def set_upstream(self, task):
        self.predecessors.append(task)


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

    def _build_graph(self):
        pass


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
    check_cycles(pipeline.tasks)


def check_cycles(tasks: List[Task]):
    G = nx.DiGraph()
    for task in tasks:
        for predecessor in task.predecessors:
            G.add_edge(predecessor, task)
    try:
        nx.find_cycle(G)
    except nx.NetworkXNoCycle:
        return None
    else:
        raise nx.HasACycle("Task have cycles!")

