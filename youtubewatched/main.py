import click

from youtubewatched.config import PORT


@click.command()
@click.option('-p', '--port', default=PORT,
              help='The port at which the server will listen', type=click.INT)
def launch(port):
    # import is here as the --help command takes way too long otherwise
    from youtubewatched.dash_layout import dash_app
    dash_app.run_server(port=port, debug=True)


if __name__ == '__main__':
    launch()
