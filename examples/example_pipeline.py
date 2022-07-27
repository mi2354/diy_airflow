from diy_airflow.data_model import Pipeline, Task
from datetime import datetime


def print_second_part():
    print("Omg, two tasks in one pipeline")


def print_hello_world():
    print(f"{datetime.now()} - Hello guys, we are executing a pipeline!")


task1 = Task(name="Task1", python_callable=print_hello_world)
task2 = Task(name="Task2", python_callable=print_second_part)

pipeline = Pipeline(name="Hello_world", schedule="* * * * *", tasks=[task1, task2])
