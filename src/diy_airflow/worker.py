from diy_airflow.state_saver import StateSaver


class Worker:
    def __init__(self, state_saver: StateSaver) -> None:
        self.state_saver = state_saver

    def run(self):
        s_task = self.state_saver.get_from_pool_ready()
        if s_task is not None:
            print(s_task)
