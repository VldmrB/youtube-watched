from youtubewatched.config import PORT, DEBUG
from youtubewatched.dash_layout import app, dash_app

from youtubewatched.manage_records.views import record_management
from youtubewatched.new_project.views import setup_new_project

app.secret_key = '23lkjhv9z8y$!gffflsa1g4[p[p]'
app.register_blueprint(record_management)
app.register_blueprint(setup_new_project)


def launch(port=PORT, debug=DEBUG):
    dash_app.run_server(port=port, debug=debug)


if __name__ == '__main__':
    launch()
