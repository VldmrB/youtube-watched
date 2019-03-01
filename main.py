from os.path import join

from dash_layout import app, dash_app
from flask_utils import get_project_dir_path_from_cookie
from utils import logging_config

from manage_records.views import record_management
from new_project.views import setup_new_project

app.secret_key = '23lkjhv9z8y$!gffflsa1g4[p[p]'
app.register_blueprint(record_management)
app.register_blueprint(setup_new_project)


@app.before_first_request
def initialize_logging():
    logging_config(join(get_project_dir_path_from_cookie(), 'events.log'))


if __name__ == '__main__':
    dash_app.run_server(5000, debug=True)
