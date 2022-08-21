from diy_airflow.state_saver import StateSaver
from diy_airflow.data_model import Status


class Worker:
    def __init__(self, state_saver: StateSaver) -> None:
        self.state_saver = state_saver

    def run(self):
        s_task = self.state_saver.get_from_pool_ready()
        if s_task is not None:
            try:
                task_id = f"{s_task['pipeline_id']}:{s_task['name']}"
            except:
                print("task failed!", flush=True)
            else:
                self.state_saver.save_status(task_id, Status.FINISHED)
