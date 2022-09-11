from diy_airflow.data_model import Pipeline, Task


def print_hello_world():
    print("Hello world!")


task1 = Task(name="Task1", python_callable=print_hello_world)

pipeline = Pipeline(name="Hello_world", schedule="* * * * *", task_list=[task1])
