from diy_airflow.data_model import Pipeline, Task


def print_hello_world():
    print("Hello guys")


task1 = Task(
    name='task1',
    python_callable=print_hello_world
)

pipeline = Pipeline(
    name = 'Hello_world',
    schedule = '* * * 1 * *',
    tasks=[task1])
