import sqlite3
import json
from os.path import join
from flask import Response, Flask
from sql_utils import sqlite_connection
from flask_utils import get_project_dir_path_from_cookie, db_has_records
from config import DB_NAME
import plotly
import dash
import dash_core_components as dcc
import dash_html_components as html
import analyze


def database_layout(db_path: str):
    decl_types = sqlite3.PARSE_DECLTYPES
    decl_colnames = sqlite3.PARSE_COLNAMES
    conn = sqlite_connection(db_path,
                             detect_types=decl_types | decl_colnames)
    # layout = html.Div('Getting ready...')
    try:
        data = analyze.retrieve_time_data(conn)
        graph = analyze.plotly_watch_chart(data)

        slider = dcc.RangeSlider(id='graph_adjuster', min=0, max=10,
                                 marks=[*range(0, 11)])
        layout = html.Div([graph, slider])
    except Exception:
        raise
    finally:
        conn.close()
    return layout


class Dashing(dash.Dash):

    def serve_layout(self):
        db_path = join(get_project_dir_path_from_cookie(), DB_NAME)
        if db_has_records():
            layout = database_layout(db_path)
        else:
            layout = self.layout

        return Response(json.dumps(layout, cls=plotly.utils.PlotlyJSONEncoder),
                        mimetype='application/json')


app = Flask(__name__)
dash_app = Dashing(__name__, server=app, routes_pathname_prefix='/dash/')
dash_app.title = 'Graphs'
dash_app.layout = html.Div('Where is.... your dataz!')
