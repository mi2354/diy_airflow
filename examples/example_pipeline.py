
def print_hello_world():
    print("Hello guys")

class MyPipeline:
    name = 'Hello_world'
    schedule = '* * * 1 * *'
    python_callable = print_hello_world
