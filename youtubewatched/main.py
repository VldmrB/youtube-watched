from os.path import join

from youtubewatched.config import PORT, DEBUG
from youtubewatched.dash_layout import app, dash_app
from youtubewatched.utils.app import get_project_dir_path_from_cookie
from youtubewatched.utils.gen import logging_config

from youtubewatched.manage_records.views import record_management
from youtubewatched.new_project.views import setup_new_project

app.secret_key = '23lkjhv9z8y$!gffflsa1g4[p[p]'
app.register_blueprint(record_management)
app.register_blueprint(setup_new_project)


def launch():
    dash_app.run_server(PORT, debug=DEBUG)


@app.before_first_request
def initialize_logging():
    logging_config(join(get_project_dir_path_from_cookie(), 'events.log'))


if __name__ == '__main__':
    launch()
