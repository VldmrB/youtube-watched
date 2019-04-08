import click

from youtubewatched.config import PORT


@click.command()
@click.option('-d', '--debug', 'debug', is_flag=True,
              help='Enable debugging mode (Flask)')
@click.option('-p', '--port', default=PORT,
              help=f'Server port (default: {PORT})')
def launch(debug, port):
    # import is here as the --help command takes way too long otherwise
    from youtubewatched.dash_layout import dash_app
    dash_app.run_server(port=port, debug=debug)


if __name__ == '__main__':
    launch()
