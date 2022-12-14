from datetime import datetime
from diy_airflow.data_model import Pipeline, Task
from time import sleep


def func7():
    print(f"{datetime.now()} - exec func7")


def func6():
    print(f"{datetime.now()} - exec func6")


def func5():
    print(f"{datetime.now()} - exec func5")


def func4():
    print(f"{datetime.now()} - exec func4")


def func3():
    print(f"{datetime.now()} - exec func3")


def func2():
    print("sleeping...")
    sleep(15)
    print(f"{datetime.now()} - exec func2")


def func1():
    print(f"{datetime.now()} - exec func1")


task1 = Task(name="task1", python_callable=func1)
task2 = Task(name="task2", python_callable=func2)
task3 = Task(name="task3", python_callable=func3)
task4 = Task(name="task4", python_callable=func4)
task5 = Task(name="task5", python_callable=func5)
task6 = Task(name="task6", python_callable=func6)
task7 = Task(name="task7", python_callable=func7)

task1.set_downstream(task3)
task1.set_downstream(task2)
task3.set_downstream(task4)
task2.set_downstream(task4)
# task4.set_downstream(task5)
task4.set_downstream(task6)
task4.set_downstream(task7)
task4.set_downstream(task5)
# task3.set_downstream(task1)


exec_time = "18/09/19 01:55:19"

pipeline = Pipeline(
    name="--Graph task--",
    task_list=[task2, task3, task4, task5, task7, task6, task1],
    schedule="* * * * *",
    start_date=(datetime.strptime(exec_time, "%d/%m/%y %H:%M:%S")),
)
