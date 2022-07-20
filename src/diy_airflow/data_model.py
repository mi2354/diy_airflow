from dataclasses import dataclass
from typing import Callable, Protocol, runtime_checkable
import croniter


@runtime_checkable
class Pipeline(Protocol):
    name: str
    schedule: str
    python_callable: Callable


def validate_pipeline(pipeline: Pipeline):
    if not isinstance(pipeline.name, str):
        raise TypeError("Pipeline name not a string")
    if not isinstance(pipeline.schedule, str):
        raise TypeError("Pipeline schedule is not a string")
    if not croniter.is_valid(pipeline.schedule):
        raise ValueError("Pipeline schedule is not a valid cron")
