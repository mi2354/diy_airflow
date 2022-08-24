# Scheduler assignment

We want to build a scheduler that can run a sequence of tasks in the right order.

The pipeline of tasks should be defined using python, using syntax like the one below

```python
pipeline = Pipeline(
    name='test',
    schedule='* * * * * *'
)

def print_hello():
    print('hello')
    
    
def print_world():
    print('world')
    

task_1 = PythonTask(
    'print_hello',
    python_callable=print_hello
    pipeline=pipeline
)

task_2 = PythonTask(
    'print_world',
    python_callable=print_world
    pipeline=pipeline
)

task_3 = HttpTask(
    'http request',
    endpoint='http://httpbin.org/get',
    method='GET'
    pipeline=pipeline
    
)

task_1.set_downstream(task_2)
task_2.set_downstream(task_3)
```

## Part 1: The scheduler

We want to be able to run a scheduler from the command line that takes in as arguments the path to a folder that contains python files with Pipeline objects. The scheduler should run continuously and wait until a new pipeline should start, as defined by its schedule cron string.

The overall usage of this command line program will be something along the lines of: (Assuming the program is called 'workflow')

```
> workflow scheduler pipelines/s
```

**hints**:

You can use the croniter package to deal with parsing the cron string


## Improvements
- wasabi doesn't have the option `flush=True`, PR to add that?
