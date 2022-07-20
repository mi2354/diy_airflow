
def print_hello_world():
    print("Hello guys")

class Pipeline:
    name = 'Hello_world'
    schedule = '* * * 1 * *'
    python_callable = print_hello_world
