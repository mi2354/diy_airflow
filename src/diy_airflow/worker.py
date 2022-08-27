from wasabi import msg

from diy_airflow.state_saver import StateSaver
from diy_airflow.data_model import Status
from diy_airflow.utils import get_pipeline_from_file


class Worker:
    def __init__(self, state_saver: StateSaver) -> None:
        self.state_saver = state_saver

    def run(self):
        s_task = self.state_saver.get_from_pool_ready()
        if s_task is not None:
            task_id = f"{s_task.pipeline_id}:{s_task.name}"
            pipeline = get_pipeline_from_file(s_task.filepath)
            if pipeline is not None:
                for task in pipeline.task_list:
                    if task.name == s_task.name:
                        try:
                            self.state_saver.save_status(task_id, Status.RUNNING)
                            task.python_callable()
                        except:
                            msg.fail("Task failed!")
                            self.state_saver.save_status(task_id, Status.FINISHED_FAIL)
                        else:
                            msg.info(f"Finished task {task_id}")
                            self.state_saver.save_status(
                                task_id, Status.FINISHED_SUCCESS
                            )
