import sqlite3
from os.path import join
from textwrap import dedent

import dash_core_components as dcc
import dash_html_components as html
import numpy as np
import pandas as pd
import plotly.graph_objs as go
from dash.dependencies import Input, Output, State
from dash_html_components import Div
from flask import Flask

import analyze
from config import DB_NAME
from dashing.overrides import Dashing
from flask_utils import get_project_dir_path_from_cookie
from get_from_sql import videos_scatter_graph
from sql_utils import sqlite_connection, execute_query

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
toggleSpikelines
"""


def get_db_path():
    return join(get_project_dir_path_from_cookie(), DB_NAME)


def format_num(num: int):
    new_num = ''
    num = str(num)[::-1]
    last_index = len(num) - 1
    for i, ltr in enumerate(num):
        new_num += ltr
        if (i + 1) % 3 == 0 and i < last_index:
            new_num += ','
    return new_num[::-1]


css = ['/static/dash_graph.css']
app = Flask(__name__)
dash_app = Dashing(__name__, server=app,
                   routes_pathname_prefix='/dash/', external_stylesheets=css)
dash_app.title = 'Graphs'

# -------------------- Layout element creation
graph_tools_to_remove = [
    'select2d', 'lasso2d', 'hoverCompareCartesian', 'resetScale2d',
    'hoverClosestCartesian', 'zoomIn2d', 'zoomOut2d', 'toggleSpikelines']

group_by_values = ['Year', 'Month', 'Day', 'Hour']
date_periods_vals = {i: group_by_values[i][0]
                     for i in range(len(group_by_values))}

# ------ Watch history graph
history_date_period_slider_id = 'date-period-slider'
history_date_period_slider_container_id = 'date-period-slider-container'
history_date_period_summary_id = 'history-date-period-summary'

group_by_slider = Div(
    [dcc.Slider(id=history_date_period_slider_id, min=0, max=3, value=1,
                marks=group_by_values)],
    style={'margin': '-400px 0 400px 65%', 'width': 150},
    id=history_date_period_slider_container_id)
history_graph_id = 'watch-history'
history_graph = Div(
    dcc.Graph(
        id=history_graph_id,
        config=dict(displaylogo=False,
                    modeBarButtonsToRemove=graph_tools_to_remove),
        style=dict(height=400)))

history_date_period_summary_msg = dcc.Markdown(
    'Click on a point on the above graph to '
    'display a summary for that period')

# ------ Scatter videos' graph, its controls and point summary 
v_scatter_graph_view_slider_marks = {1: '1', 100_000: '100K', 1_000_000: '1M',
                                     10_000_000: '10M', 100_000_000: '100M',
                                     10_000_000_000: '10B',
                                     50_000_000_000: '50B'}
v_scatter_graph_view_range_nums = list(v_scatter_graph_view_slider_marks.keys())
v_scatter_graph_slider_container_id = 'scatter-graph-slider-container'
v_scatter_graph_x_dropdown_id = 'scatter-graph-x-dropdown'
v_scatter_graph_y_dropdown_id = 'scatter-graph-y-dropdown'
v_scatter_graph_slider_id = 'scatter-graph-slider'
v_scatter_graph_summary_id = 'scatter-graph-hover-summary'
v_scatter_graph_container_id = 'scatter-graph-container'
v_scatter_graph_id = 'scatter-graph'

v_scatter_graph_slider = html.Div(
    [dcc.RangeSlider(id=v_scatter_graph_slider_id,
                     min=0, max=len(v_scatter_graph_view_slider_marks) - 1,
                     value=[1, 3],
                     marks=list(v_scatter_graph_view_slider_marks.values()))],
    id=v_scatter_graph_slider_container_id, style={'width': 800})

scatter_dropdown_style = {'display': 'flex',
                          'justify-content': 'space-between',
                          'align-items': 'center',
                          'margin': 5,
                          'width': 350}

x_axis_list = {
    'Likes/dislikes ratio (highest)': 'LikeRatioDesc',
    'Likes/dislikes ratio (lowest)': 'LikeRatioAsc',
    'Views': 'Views',
    'Tag count': 'TagCount',
    'Duration': 'Duration',
    'Comment count': 'CommentCount',
    'Title length': 'TitleLength',
}

y_axis_list = {
    'Likes/dislikes ratio': 'Ratio',
    'Views': 'Views',
    'Tag count': 'TagCount',
    'Duration': 'Duration',
    'Comment count': 'CommentCount',
    'Title length': 'TitleLength',
}

v_scatter_graph_x_dropdown = Div(
    children=['X axis: ', dcc.Dropdown(
        value='LikeRatioDesc',
        options=[{'label': k, 'value': v} for k, v in x_axis_list.items()],
        style={'width': 300},
        id=v_scatter_graph_x_dropdown_id)],
    style=scatter_dropdown_style)

v_scatter_graph_y_dropdown = Div(
    children=['Y axis: ', dcc.Dropdown(
        value='Views',
        options=[{'label': k, 'value': v} for k, v in y_axis_list.items()],
        style={'width': 300},
        id=v_scatter_graph_y_dropdown_id)],
    style=scatter_dropdown_style)

v_scatter_graph = Div(
    dcc.Graph(
        id=v_scatter_graph_id,
        config=dict(displaylogo=False,
                    modeBarButtonsToRemove=graph_tools_to_remove),
        style=dict(height=500, width=800)),
    style=dict(display='inline-block'))

v_scatter_graph_section = Div(
    [
        Div(
            children=[
                v_scatter_graph_x_dropdown,
                v_scatter_graph_y_dropdown
            ],
            style={
                'display': 'flex',
                'justify-content': 'space-between',
                'width': 720
            }),
        Div(
            children=[
                v_scatter_graph,
                Div(
                    id=v_scatter_graph_summary_id,
                    children='Mouseover summary'
                )
            ],
            style={'display': 'flex'}
        ),
        v_scatter_graph_slider,
    ],
    style={'margin': 15},
    id='scatter-graph-section-container'
)

# ------ Layout organizing/setting {Final}
dash_app.layout = Div(
    [
        # introductory history graph
        Div(history_graph), Div(group_by_slider),
        # summary for a clicked on point on the above graph (date period)
        Div(history_date_period_summary_msg,
            id=history_date_period_summary_id,
            style={'display': 'flex',
                   'margin': '0 15px'}
            ),
        v_scatter_graph_section
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
def history_chart_date_summary(data, date_period):
    if data:
        date = data['points'][0]['x']
        if data['points'][0]['y'] == 0:
            return f'No videos for the period of {date}'
        if date_period == 0:
            date = date[:4]
        elif date_period == 1:
            date = date[:7]
        elif date_period == 2:
            date = date[:10]
        else:
            date = date[:13]

        db_path = join(get_project_dir_path_from_cookie(), DB_NAME)
        conn = sqlite_connection(db_path)
        summary_tables = analyze.retrieve_data_for_a_date_period(conn, date)
        conn.close()
        return [*summary_tables]
    else:
        return history_date_period_summary_msg


def construct_v_scatter_graph(df: pd.DataFrame,
                              x_axis_col: str, y_axis_col: str):
    clr_dict = {k: v for k, v in zip(df.Channel.unique(),
                                     list(range(len(df.Channel.unique()))))}
    marker_options = {'color': [clr_dict[i] for i in df.Channel],
                      'colorscale': 'Electric', 'opacity': 0.7, 'size': 8
                      }
    data = [go.Scatter(x=df[x_axis_col], y=df[y_axis_col], mode='markers',
                       marker=marker_options,
                       customdata=df.VideoID,
                       showlegend=True,
                       name=''
                       )]
    layout = go.Layout(
        margin=dict.fromkeys(list('ltrb'), 40),
        hovermode='closest', colorscale=go.layout.Colorscale()
    )
    return go.Figure(data=data, layout=layout)


@dash_app.callback(Output(v_scatter_graph_id, 'figure'),
                   [
                       Input(v_scatter_graph_x_dropdown_id, 'value'),
                       Input(v_scatter_graph_y_dropdown_id, 'value'),
                       Input(v_scatter_graph_slider_id, 'value'),
                   ])
def update_v_scatter_graph(x_axis_type: str, y_axis_type: str, views):
    if not x_axis_type or not y_axis_type:
        return None
    min_views = v_scatter_graph_view_range_nums[views[0]]
    max_views = v_scatter_graph_view_range_nums[views[1]]
    db_path = join(get_project_dir_path_from_cookie(), DB_NAME)
    conn = sqlite_connection(db_path)
    data = videos_scatter_graph.get_data(conn, x_axis_type, y_axis_type,
                                         min_views=min_views,
                                         max_views=max_views)
    if x_axis_type in ['LikeRatioAsc', 'LikeRatioDesc']:
        x_axis_type = 'Ratio'
    conn.close()
    return construct_v_scatter_graph(data, x_axis_type, y_axis_type)


@dash_app.callback(Output(v_scatter_graph_summary_id, 'children'),
                   [Input(v_scatter_graph_id, 'hoverData')])
def update_v_scatter_graph_summary(hover_data: dict):
    if hover_data:
        point_of_interest = hover_data['points'][0]['customdata']
    else:
        point_of_interest = None

    if point_of_interest:
        conn = sqlite_connection(get_db_path())
        '''Views, Ratio, Likes, Dislikes, Title, Channel, Upload date'''
        query = '''SELECT v.title, c.title, v.published_at, v.view_count,
        v.like_count, v.dislike_count, (v.like_count * 1.0 / dislike_count),
        v.status, v.last_updated FROM
        videos v JOIN channels c ON v.channel_id = c.id
        WHERE v.id = ?'''
        r = execute_query(conn, query, (point_of_interest,))[0]
        conn.close()

        views = format_num(r[3])
        likes = format_num(r[4])
        dislikes = format_num(r[5])
        status = f'**Status:** {r[7]}'
        if r[7] == 'active':
            status += f', last checked on {r[8][:10]}'

        like_dislike_ratio = round(r[6], 2) if r[6] else 'n/a'
        return dcc.Markdown(dedent(
            f'''**Title:** {r[0]}    
                    **Channel:** {r[1]}    
                    **Published:** {r[2][:10]}    
                    **Views:** {views}    
                    **Likes:** {likes}    
                    **Dislikes:** {dislikes}    
                    **Ratio:** {like_dislike_ratio}    
                    **ID:** {point_of_interest}    
                    {status}
                '''))
