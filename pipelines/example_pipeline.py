from datetime import datetime

from diy_airflow.data_model import Pipeline, Task


def print_second_part():
    print("Omg, two tasks in one pipeline! new!", flush=True)


def print_hello_world():
    print(f"{datetime.now()} - Hello guys, we are executing a pipeline!")


task1 = Task(name="Task1", python_callable=print_hello_world)
task2 = Task(name="Task2", python_callable=print_second_part)
task1.set_downstream(task2)

pipeline = Pipeline(name="Hello_world", schedule="* * * * *", task_list=[task1, task2])
