import click


@click.command()
@click.argument('path', type=str)
def main(path):
    click.echo(f'Selected path: {path}')
    

 
if __name__ == '__main__':
    main()