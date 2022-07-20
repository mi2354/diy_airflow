from diy_airflow.data_model import Pipeline
def print_hello_world():
    print("Hello guys")

pipeline = Pipeline(
    name = 'Hello_world',
    schedule = '* * * 1 * *',
    python_callable = print_hello_world)
