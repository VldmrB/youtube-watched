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
from dash.dependencies import Input, Output, State
from dash_html_components import Div

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
        if db_has_records():
            layout = database_layout()
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
dash_app.layout = Div('Where is.... your dataz!')

group_by_values = ['Year', 'Month', 'Day', 'Hour']
date_periods_vals = {i: group_by_values[i][0]
                     for i in range(len(group_by_values))}
summary_msg = dcc.Markdown('Click on a daily/hourly point on the graph to '
                           'display a **summary** for it')


def chart_history(data: pd.DataFrame, date_period='M'):
    title = 'Videos opened/watched'
    if date_period == 'Y':
        data = data.groupby(pd.Grouper(freq='YS')).aggregate(np.sum)
    elif date_period == 'M':
        data = data.groupby(pd.Grouper(freq='MS')).aggregate(np.sum)
    elif date_period == 'D':
        data = data.groupby(pd.Grouper(freq='D')).aggregate(np.sum)

    data = data.reset_index()
    data = [go.Scatter(x=data.watched_at.values.astype('str'), y=data.times,
                       mode='lines')]
    layout = go.Layout(
        title=title,
        yaxis=go.layout.YAxis(fixedrange=True),
        margin=dict.fromkeys(list('ltrb'), 30),
        hovermode='closest'
    )
    return {'data': data, 'layout': layout}


def database_layout():
    group_by_slider = html.Div(
        [dcc.Slider(id='date-period-slider', min=0, max=3, value=1,
                    marks=['Year', 'Month', 'Day', 'Hour'])],
        id='graph-adjuster',)
    graph_1 = Div(
        dcc.Graph(
            id='watch-history',
            config=dict(displaylogo=False,
                        modeBarButtonsToRemove=[
                            'select2d', 'lasso2d', 'hoverCompareCartesian',
                            'hoverClosestCartesian', 'zoomIn2d', 'zoomOut2d']),
            style=dict(height=400)))

    layout = Div(
        [
            Div(graph_1), Div(group_by_slider),
            Div(summary_msg, id='summary')
         ])
    return layout


@dash_app.callback(Output('watch-history', 'figure'),
                   [Input('date-period-slider', 'value')])
def update_history_chart(value):
    db_path = join(get_project_dir_path_from_cookie(), DB_NAME)
    decl_types = sqlite3.PARSE_DECLTYPES
    decl_colnames = sqlite3.PARSE_COLNAMES
    conn = sqlite_connection(db_path,
                             detect_types=decl_types | decl_colnames)
    data = analyze.retrieve_watch_data(conn)
    conn.close()
    return chart_history(data, date_periods_vals[value])


@dash_app.callback(Output('summary', 'children'),
                   [Input('watch-history', 'clickData')],
                   [State('date-period-slider', 'value')])
def show_summary(data, date_period):
    # and date_period in [2, 3]
    if data:  # Day and Hour values respectively
        if date_period == 0:
            date = data['points'][0]['x'][:4]
        elif date_period == 1:
            date = data['points'][0]['x'][:7]
        elif date_period == 2:
            date = data['points'][0]['x'][:10]
        # elif date_period == 3:
        else:
            date = data['points'][0]['x'][:13]

        db_path = join(get_project_dir_path_from_cookie(), DB_NAME)
        conn = sqlite_connection(db_path)
        datum = analyze.retrieve_data_for_a_date_period(conn, date)
        conn.close()
        return Div([datum])
    else:
        return summary_msg
