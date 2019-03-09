import json
import sqlite3
from os.path import join
from dataclasses import dataclass

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


@dataclass(init=False, repr=False, eq=False, order=False, unsafe_hash=False)
class DataCache:
    ld_ratio: pd.DataFrame = None


class Dashing(dash.Dash):

    def serve_layout(self):
        if db_has_records():
            layout = self.layout
        else:
            layout = Div(['No data found. Make sure to first import your ',
                          html.A('Takeout data', href='http://127.0.0.1:5000')])

        return Response(json.dumps(layout, cls=plotly.utils.PlotlyJSONEncoder),
                        mimetype='application/json')


css = ['/static/dash_graph.css']
app = Flask(__name__)
dash_app = Dashing(__name__, server=app,
                   routes_pathname_prefix='/dash/', external_stylesheets=css)
dash_app.title = 'Graphs'

# -------------------- Layout element creation
graph_tools_to_remove = [
    'select2d', 'lasso2d', 'hoverCompareCartesian',
    'hoverClosestCartesian', 'zoomIn2d', 'zoomOut2d']

group_by_values = ['Year', 'Month', 'Day', 'Hour']
date_periods_vals = {i: group_by_values[i][0]
                     for i in range(len(group_by_values))}

# ------ Watch history graph layout
history_date_period_slider_id = 'date-period-slider'
history_date_period_slider_container_id = 'date-period-slider-container'
history_date_period_summary_id = 'history-date-period-summary'

group_by_slider = html.Div(
    [dcc.Slider(id=history_date_period_slider_id, min=0, max=3, value=1,
                marks=group_by_values)],
    id=history_date_period_slider_container_id)
history_graph_id = 'watch-history'
history_graph = Div(
    dcc.Graph(
        id=history_graph_id,
        config=dict(displaylogo=False,
                    modeBarButtonsToRemove=graph_tools_to_remove),
        style=dict(height=400)))

# ------ Most disliked/liked videos graphs layout
ratio_graphs_slider_marks = {1: '1', 100_000: '100K', 1_000_000: '1M',
                             10_000_000: '10M', 100_000_000: '100M',
                             10_000_000_000: '10B', 100_000_000_000: '100B'}
ratio_graphs_slider_container_id = 'ratio-graphs-slider-container'
ratio_graphs_slider_id = 'ratio-graphs-slider'
ratio_graphs_summary_id = 'ratio-graph-hover-summary'
ratio_graphs_container_id = 'ratio-graphs-container'
ratio_graphs_type_radio_id = 'ratio-graphs-type'
liked_ratio_graph_id = 'liked-ratio-graph'
disliked_ratio_graph_id = 'disliked-ratio-graph'

ratio_graphs_type_radio = dcc.RadioItems(
                id=ratio_graphs_type_radio_id,
                options=[{'label': i, 'value': i} for i in
                         ['Videos', 'Channels']],
                value='Videos',
                labelStyle={'display': 'inline-block'})


ratio_graphs_slider = html.Div(
    [dcc.RangeSlider(id=ratio_graphs_slider_id,
                     min=0, max=len(ratio_graphs_slider_marks)-1, value=[1, 3],
                     marks=list(ratio_graphs_slider_marks.values()))],
    id=ratio_graphs_slider_container_id)
liked_ratio_graph = Div(dcc.Graph(
        id=liked_ratio_graph_id,
        config=dict(displaylogo=False,
                    modeBarButtonsToRemove=graph_tools_to_remove),
        style=dict(height=475, width=475)), style=dict(display='inline-block'))
disliked_ratio_graph = Div(dcc.Graph(
        id=disliked_ratio_graph_id,
        config=dict(displaylogo=False,
                    modeBarButtonsToRemove=graph_tools_to_remove),
        style=dict(height=475, width=475)), style=dict(display='inline-block'))


history_date_period_summary_msg = dcc.Markdown(
    'Click on a point on the graph above to '
    'display a summary for that period')

# ------ Layout organizing/element setting

dash_app.layout = Div(
    [
        # introductory history graph
        Div(history_graph), Div(group_by_slider),
        # summary for a clicked on point on the above graph (date period)
        Div(history_date_period_summary_msg, id=history_date_period_summary_id),
        # ratio graphs, point summary and their controls
        Div([
            Div(ratio_graphs_type_radio),  # radio choice, videos/channels
            liked_ratio_graph,
            disliked_ratio_graph,
            Div(id=ratio_graphs_summary_id,
                style=dict(display='inline-block')),
            ratio_graphs_slider
        ], style=dict(clear='both')
        )
    ])
# -------------------- Layout {End}


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


@dash_app.callback(Output(history_graph_id, 'figure'),
                   [Input(history_date_period_slider_id, 'value')])
def update_history_chart(value):
    db_path = join(get_project_dir_path_from_cookie(), DB_NAME)
    decl_types = sqlite3.PARSE_DECLTYPES
    decl_colnames = sqlite3.PARSE_COLNAMES
    conn = sqlite_connection(db_path,
                             detect_types=decl_types | decl_colnames)
    data = analyze.retrieve_watch_data(conn)
    conn.close()
    return chart_history(data, date_periods_vals[value])


@dash_app.callback(Output(history_date_period_summary_id, 'children'),
                   [Input(history_graph_id, 'clickData')],
                   [State(history_date_period_slider_id, 'value')])
def show_summary(data, date_period):
    if data:
        if date_period == 0:
            date = data['points'][0]['x'][:4]
        elif date_period == 1:
            date = data['points'][0]['x'][:7]
        elif date_period == 2:
            date = data['points'][0]['x'][:10]
        else:
            date = data['points'][0]['x'][:13]

        db_path = join(get_project_dir_path_from_cookie(), DB_NAME)
        conn = sqlite_connection(db_path)
        summary_tables = analyze.retrieve_data_for_a_date_period(conn, date)
        conn.close()
        return [*summary_tables]
    else:
        return history_date_period_summary_msg


def ratios_graphs(data: pd.DataFrame, liked: bool):
    if liked:
        title = 'Highest like/dislike ratio'
    else:
        title = 'Lowest like/dislike ratio'

    if 'Title' in data.columns:
        text = [f'Title: {i[0]}<br>Channel: {i[1]}<br>Uploaded: {i[2][:10]}'
                for i in data.loc[:, 'Title':'PublishDate'].values]
        hover_template = ('Views: %{x:2f}<br>'
                          'Ratio: %{y}<br>'
                          '%{text}')
    else:
        text = [f'Channel: {i}'
                for i in data.loc[:, 'Channel'].values]
        hover_template = ('Views: %{x:2f}<br>'
                          'Ratio: %{y}<br>'
                          '%{text}')
    data = [go.Scatter(x=data.Views, y=data.Ratio, mode='markers',
                       text=text, hovertemplate=hover_template,
                       showlegend=False,
                       name=''  # counteract the tooltip showing the trace name,
                       # despite one not being explicitly set, brought on by
                       # use of hovertemplate
                       )]
    layout = go.Layout(
        title=title,
        xaxis=go.layout.XAxis(title='Views'),
        yaxis=go.layout.YAxis(title='Like/dislike ratio'),
        margin=dict.fromkeys(list('ltrb'), 40),
        hovermode='closest', colorscale=go.layout.Colorscale()
    )
    return go.Figure(data=data, layout=layout)


@dash_app.callback(Output(liked_ratio_graph_id, 'figure'),
                   [Input(ratio_graphs_slider_id, 'value'),
                    Input(ratio_graphs_type_radio_id, 'value')])
def update_liked_ratio_videos_graph(views, ratio_type):
    pick_from = list(ratio_graphs_slider_marks.keys())
    db_path = join(get_project_dir_path_from_cookie(), DB_NAME)
    conn = sqlite_connection(db_path)
    data = analyze.top_liked_or_disliked_videos_or_channels_by_ratio(
        conn, ratio_type, True, 100, pick_from[views[0]], pick_from[views[1]])
    conn.close()
    return ratios_graphs(data, True)


@dash_app.callback(Output(disliked_ratio_graph_id, 'figure'),
                   [Input(ratio_graphs_slider_id, 'value'),
                    Input(ratio_graphs_type_radio_id, 'value')])
def update_disliked_ratio_videos_graph(views, ratio_type):
    pick_from = list(ratio_graphs_slider_marks.keys())
    db_path = join(get_project_dir_path_from_cookie(), DB_NAME)
    conn = sqlite_connection(db_path)
    data = analyze.top_liked_or_disliked_videos_or_channels_by_ratio(
        conn, ratio_type, False, 100, pick_from[views[0]], pick_from[views[1]])
    conn.close()
    return ratios_graphs(data, False)


@dash_app.callback(Output(ratio_graphs_summary_id, 'children'),
                   [Input(liked_ratio_graph_id, 'hoverData'),
                    Input(disliked_ratio_graph_id, 'hoverData')])
def ratio_graph_hover_summary(liked_hover_data: dict,
                              disliked_hover_data: dict):
    pass

