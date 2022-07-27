from datetime import datetime

from diy_airflow.data_model import Pipeline, Task


def func2():
    print(f"{datetime.now()} - exec func2")


def func1():
    print(f"{datetime.now()} - exec func1")


task1 = Task(name="task1", python_callable=func1)
task2 = Task(name="task2", python_callable=func1)
task1.set_upstream(task2)

exec_time = '18/09/19 01:55:19'

pipeline = Pipeline(
    name="--Graph task--",
    tasks=[task1, task2],
    schedule= "* * * * *",
    start_date=(datetime.strptime(exec_time, '%d/%m/%y %H:%M:%S'))
)
