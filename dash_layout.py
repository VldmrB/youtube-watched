import json
import sqlite3
from os.path import join

import dash
import dash_core_components as dcc
import dash_html_components as html
import numpy as np
import pandas as pd
import plotly
import plotly.graph_objs as go
from flask import Response, Flask
from dash.dependencies import Input, Output

import analyze
from config import DB_NAME
from flask_utils import get_project_dir_path_from_cookie, db_has_records
from sql_utils import sqlite_connection

"""
zoom2d
pan2d
select2d
lasso2d
zoomIn2d
zoomOut2d
sendDataToCloud
toImage
autoScale2d
resetScale2d
hoverClosestCartesian
hoverCompareCartesian
"""


class Dashing(dash.Dash):

    def serve_layout(self):
        db_path = join(get_project_dir_path_from_cookie(), DB_NAME)
        if db_has_records():
            layout = database_layout(db_path)
        else:
            layout = self.layout

        return Response(json.dumps(layout, cls=plotly.utils.PlotlyJSONEncoder),
                        mimetype='application/json')


css = ['/static/dash_graph.css']
app = Flask(__name__)
dash_app = Dashing(__name__, server=app,
                   routes_pathname_prefix='/dash/', external_stylesheets=css)
dash_app.config['suppress_callback_exceptions'] = True  # since the real layout
# only gets set during the actual page request, the initial one has no ids
# required for the callbacks
dash_app.title = 'Graphs'
dash_app.layout = html.Div('Where is.... your dataz!')


def plotly_watch_chart(data: pd.DataFrame, date_interval='D'):
    df = data.groupby(pd.Grouper(freq=date_interval)).aggregate(np.sum)
    df = df.reset_index()
    graph = dcc.Graph(id='le-graph',
                      figure=go.Figure(
                          data=[go.Scatter(x=df.watched_at, y=df.times,
                                           mode='lines+markers')],
                          layout=go.Layout(
                              xaxis=go.layout.XAxis(fixedrange=True),
                              yaxis=go.layout.YAxis(fixedrange=True))),
                      config=dict(
                          displaylogo=False,
                          modeBarButtonsToRemove=['select2d', 'lasso2d']),
                      style={'width': 600})
    return graph


def database_layout(db_path: str):
    decl_types = sqlite3.PARSE_DECLTYPES
    decl_colnames = sqlite3.PARSE_COLNAMES
    conn = sqlite_connection(db_path,
                             detect_types=decl_types | decl_colnames)
    try:
        data = analyze.retrieve_time_data(conn)
        graph = html.Div(plotly_watch_chart(data, 'YS'))
        slider = html.Div(
            [html.Div('Set a date period to sort by', id='date-period-slider'),
             dcc.Slider(min=0, max=3, marks=['Year', 'Month', 'Day', 'Hour'])],
            id='graph-adjuster')
        layout = html.Div([graph, slider])
    except Exception:
        raise
    finally:
        conn.close()
    return layout


@dash_app.callback(Output('le-graph', 'children'), [Input('', 'value')])
def update_history_chart():
    pass
