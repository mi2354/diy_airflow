from typing import List, Union
from datetime import datetime
from queue import PriorityQueue
from typing import Optional

from croniter import croniter
from wasabi import msg

from diy_airflow.data_model import Pipeline, Status, SimplePipeline, SimpleTask
from diy_airflow.state_saver import StateSaver


class Scheduler:
    def __init__(self, state_saver: Union[bool, StateSaver] = False) -> None:
        self.state_saver = state_saver
        self.q = PriorityQueue(maxsize=100)
        self.queued_ids = []
        self.todo_pool: List[SimplePipeline] = []

    def add_pipeline(self, pipeline: Optional[Pipeline]) -> None:
        """
        Add pipeline to queue
        """
        if isinstance(pipeline, Pipeline):
            # TODO: There might be a bug here with start_dates, since the new pipeline
            # has the initial start_dates
            pipeline.build_ids()
            if pipeline.name not in self.queued_ids:
                self.q.put(pipeline)
                self.queued_ids.append(pipeline.name)
            else:
                new_queue = PriorityQueue(maxsize=100)
                while not self.q.empty():
                    queued_pipeline = self.q.get()
                    if queued_pipeline.name == pipeline.name:
                        new_queue.put(pipeline)
                    else:
                        new_queue.put(queued_pipeline)
                self.q = new_queue

    def schedule_pipelines(self):
        """
        Check next pipeline to be executed in the queue and schedule it if possible
        """
        if not self.q.empty():
            pipeline: Pipeline = self.q.get()

            pipeline_start_date = pipeline.start_date
            if pipeline_start_date < datetime.now():
                try:
                    todo_element = SimplePipeline(
                        id=pipeline.id_,
                        filepath=pipeline.filepath,
                        graph=pipeline.G.copy(),
                    )
                    self.todo_pool.append(todo_element)
                except Exception as e:
                    msg.fail(f"Failed to schedule pipeline {pipeline.name}", flush=True)
                    print(e)
                else:
                    msg.info(f"Pipeline {pipeline.name} scheduled!")
                    if self.state_saver:
                        self.state_saver.save_pipeline_run(pipeline)
                iter = croniter(pipeline.schedule, pipeline_start_date)
                next_datetime = iter.get_next(datetime)
                # With this loop we avoid that a long task blocks our schedule
                while next_datetime < datetime.now():
                    next_datetime = iter.get_next(datetime)
                pipeline.start_date = next_datetime
                pipeline.build_ids()
            self.q.put(pipeline)

    def schedule_tasks(self):
        """
        Check if there are new nodes in the graphs that have no predecessor
        """
        for s_pipeline in self.todo_pool:
            predecessors = s_pipeline.graph.pred
            for task in predecessors:
                if (
                    not predecessors[task]
                    and self.state_saver.check_status(f"{s_pipeline.id}:{task}")
                    == Status.NOT_SCHEDULED
                ):
                    s_task = SimpleTask(
                        pipeline_id=s_pipeline.id,
                        name=task,
                        filepath=s_pipeline.filepath,
                    )
                    self.state_saver.add_to_pool_ready(s_task)
                    self.state_saver.save_status(
                        f"{s_pipeline.id}:{task}", Status.WAITING
                    )

    def update_finished(self):
        to_delete = []
        for i, s_pipeline in enumerate(self.todo_pool):
            for node in s_pipeline.graph:
                status = self.state_saver.check_status(f"{s_pipeline.id}:{node}")
                if status == Status.FINISHED_SUCCESS:
                    to_delete.append([i, node])
                elif status == Status.FINISHED_FAIL:
                    # If failed, we don't want to continue with the pipeline,
                    # so we add all nodes to to_delete
                    for n in s_pipeline.graph:
                        to_delete.append([i, n])

        for element in to_delete:
            if (
                element[1] in self.todo_pool[element[0]].graph
            ):  # Ugly, need to check instances
                self.todo_pool[element[0]].graph.remove_node(element[1])

        finished = [x for x in self.todo_pool if x.graph.number_of_nodes() == 0]
        if finished:
            for s_pipeline in finished:
                msg.good(f"Finished pipeline: {s_pipeline.id}")
        self.todo_pool = [x for x in self.todo_pool if x not in finished]

    def run(self):
        self.schedule_pipelines()
        self.schedule_tasks()
        self.update_finished()
