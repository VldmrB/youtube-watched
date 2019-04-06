import click

from youtubewatched.config import PORT
from youtubewatched.dash_layout import dash_app


@click.command()
@click.option('-p', '--port', default=PORT,
              help='The port at which the server will listen', type=click.INT)
def launch(port):
    dash_app.run_server(port=port, debug=True)
