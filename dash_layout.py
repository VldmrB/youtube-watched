import sqlite3
from os.path import join
from textwrap import dedent

import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import plotly.graph_objs as go
from dash.dependencies import Input, Output, State
from dash_html_components import Div
from flask import Flask
from plotly.colors import PLOTLY_SCALES, find_intermediate_color

from config import DB_NAME
from dashing.overrides import Dashing
from get_data import history_chart, videos_scatter_graph, tracking
from utils.app import get_project_dir_path_from_cookie, get_db_path
from utils.sql import sqlite_connection, execute_query

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


def get_colors_from_colorscale(color_candidates: dict):
    color_scale = PLOTLY_SCALES['Rainbow']
    color_scale_floats = [i[0] for i in color_scale]
    color_scale_rgbs = [i[1] for i in color_scale]
    color_scale = {k: v for k, v in PLOTLY_SCALES['Rainbow']}
    float_rgb_dict = dict()
    for k, v in color_candidates.items():
        if v in color_scale_floats:
            float_rgb_dict[k] = color_scale[v]
        else:
            for i, s_v in enumerate(color_scale_floats):
                if v < s_v:
                    float_rgb_dict[k] = find_intermediate_color(
                        color_scale_rgbs[i-1],
                        color_scale_rgbs[i],
                        v,
                        'rgb')
                    break
    return float_rgb_dict


def add_commas_to_num(num: int):
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
dash_app.config['suppress_callback_exceptions'] = True
dash_app.title = 'Graphs'

# -------------------- Layout element creation
graph_tools_to_remove = [
    'select2d', 'lasso2d', 'hoverCompareCartesian', 'resetScale2d',
    'hoverClosestCartesian', 'zoomIn2d', 'zoomOut2d', 'toggleSpikelines']

# ------ Small summary
fake_input_id = 'fake-input'
fake_input = Div('Yes', style={'display': 'none'}, id=fake_input_id)
basic_summary_text_id = 'basic-summary'
basic_summary_text = Div(id=basic_summary_text_id)
basic_summary = Div([html.H3('Basic stats'), basic_summary_text, fake_input],
                    style={'margin': '5px 15px'})

# ------ Watch history graph
group_by_values = ['Year', 'Month', 'Day', 'Hour']
date_periods_vals = {i: group_by_values[i][0]
                     for i in range(len(group_by_values))}
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
    children=[
        html.H3('Videos opened/watched'),
        dcc.Graph(
            id=history_graph_id,
            config=dict(displaylogo=False,
                        modeBarButtonsToRemove=graph_tools_to_remove),
            style={'height': 400}
            )], style={'margin': '5px 15px'})

history_date_period_summary_msg = dcc.Markdown(
    'Click on a point on the above graph to '
    'display a summary for that period')

# ------ Scatter videos' graph, its controls and point summary 
v_scatter_view_slider_marks = {1: '1', 100_000: '100K', 1_000_000: '1M',
                               10_000_000: '10M', 100_000_000: '100M',
                               10_000_000_000: '10B',
                               50_000_000_000: '50B'}
v_scatter_view_range_nums = list(v_scatter_view_slider_marks.keys())
v_scatter_views_slider_container_id = 'v-scatter-graph-slider-container'
v_scatter_views_slider_id = 'v-scatter-graph-slider'
# ---
v_scatter_recs_slider_marks = {50: '50', 100: '100', 1_000: '1K',
                               10_000: '10K', 100_000_000: 'All'}
v_scatter_slider_recs_nums = list(v_scatter_recs_slider_marks.keys())
v_scatter_slider_recs_container_id = 'v-scatter-graph-recs-slider-container'
v_scatter_recs_id = 'v-scatter-graph-recs-slider'

v_scatter_x_dropdown_id = 'v-scatter-graph-x-dropdown'
v_scatter_y_dropdown_id = 'v-scatter-graph-y-dropdown'
v_scatter_summary_id = 'v-scatter-graph-hover-summary'
v_scatter_container_id = 'v-scatter-graph-container'
v_scatter_id = 'v-scatter-graph'

v_scatter_views_slider = html.Div(
    [dcc.RangeSlider(id=v_scatter_views_slider_id,
                     min=0, max=len(v_scatter_view_slider_marks) - 1,
                     value=[1, 3],
                     marks=list(v_scatter_view_slider_marks.values()))],
    id=v_scatter_views_slider_container_id, style={'width': 800})

v_scatter_recs_slider = html.Div(
    [dcc.Slider(id=v_scatter_recs_id,
                min=0, max=len(v_scatter_recs_slider_marks) - 1,
                value=1,
                marks=list(v_scatter_recs_slider_marks.values()))],
    id=v_scatter_slider_recs_container_id, style={'width': 800})

v_scatter_dropdown_style = {'display': 'flex',
                            'justify-content': 'space-between',
                            'align-items': 'center',
                            'margin': 5,
                            'width': 350}

v_scatter_x_axis_list = {
    'Likes/dislikes ratio (highest)': 'LikeRatioDesc',
    'Likes/dislikes ratio (lowest)': 'LikeRatioAsc',
    'Views': 'Views',
    'Tag count': 'TagCount',
    # 'Duration': 'Duration',
    'Comment count': 'CommentCount',
    # 'Title length': 'TitleLength',
}

v_scatter_y_axis_list = {
    'Likes/dislikes ratio': 'Ratio',
    'Views': 'Views',
    'Tag count': 'TagCount',
    'Duration': 'Duration',
    'Comment count': 'CommentCount',
    'Title length': 'TitleLength',
}

v_scatter_x_dropdown = Div(
    children=['X axis: ', dcc.Dropdown(
        value='LikeRatioDesc',
        options=[{'label': k, 'value': v}
                 for k, v in v_scatter_x_axis_list.items()],
        style={'width': 300},
        id=v_scatter_x_dropdown_id)],
    style=v_scatter_dropdown_style)

v_scatter_y_dropdown = Div(
    children=['Y axis: ', dcc.Dropdown(
        value='Views',
        options=[{'label': k, 'value': v}
                 for k, v in v_scatter_y_axis_list.items()],
        style={'width': 300},
        id=v_scatter_y_dropdown_id)],
    style=v_scatter_dropdown_style)

v_scatter = Div(
    style=dict(display='inline-block'),
    id=v_scatter_container_id)

v_scatter_section = Div(
        [
            html.H3('Videos\' graphs'),
            Div(
                children=[
                    v_scatter_x_dropdown,
                    v_scatter_y_dropdown
                ],
                style={
                    'display': 'flex',
                    'justify-content': 'space-between',
                    'width': 720
                }),
            Div(
                children=[
                    v_scatter,
                    Div(
                        id=v_scatter_summary_id,
                        style={'margin': '0 20px'}
                    ),
                ],
                style={'display': 'flex'}
            ),
            Div('Min - Max views per video', style={'margin': '10px 0 5px 0'}),
            v_scatter_views_slider,
            Div('Number of records (note: 10K+ takes a while to render, '
                'is not interactive and will slow down the page while active)',
                style={'margin': '25px 0 5px 0'}),
            v_scatter_recs_slider
        ],
        style={'margin': 15},
        id='v-scatter-graph-section-container'
    )

# ------ Tracking channels, videos and tags over time
tracking_container_id = 'tag-tracking-container'
tracking_choice_id = 'tracking-choice'
tracking_table_container_id = 'tag-table-container'
tracking_graph_container_id = 'tag-tracking-graph-container'
tracking_graph_id = 'tag-tracking-graph'

tracking_choice = dcc.RadioItems(
    id=tracking_choice_id,
    options=[{'label': i, 'value': i} for i in ['Channels', 'Tags', 'Videos',
                                                'Topics', 'Categories']],
    value='Channels',
    labelStyle={'display': 'inline-block'}
)
tracking_table_container = Div(id=tracking_table_container_id)
tracking_graph_container = Div(id=tracking_graph_container_id,
                               )
tracking_graph = dcc.Graph(
    id=tracking_graph_id,
    style={'height': 377, 'width': 700, 'margin': 5},
    config=dict(displaylogo=False,
                modeBarButtonsToRemove=graph_tools_to_remove),
                           )
tracking_container = Div(
    children=[tracking_table_container, tracking_graph],
    id=tracking_container_id,
    style={'display': 'flex'})
tracking_section = Div(children=[html.H3('Top watched'),
                                 tracking_choice,
                                 tracking_container],
                       style={'margin': '50px 15px'})


# ------ Layout organizing/setting {Final}
dash_app.layout = Div(
    [
        Div(id='disclaimer-accuracy'),
        # basic stats
        basic_summary,
        # introductory history graph
        Div(history_graph), Div(group_by_slider),
        # summary for a clicked on point on the above graph (date period)
        Div(history_date_period_summary_msg,
            id=history_date_period_summary_id,
            style={'display': 'flex',
                   'margin': '0 15px'}
            ),
        # exploratory scatter graph for videos with changeable axis
        v_scatter_section,
        # top tags/ tag tracking graph section
        tracking_section
    ])
# -------------------- Layout {End}


@dash_app.callback(Output(basic_summary_text_id, 'children'),
                   [Input(fake_input_id, 'children')])
def basic_stats(fake_input_content):
    if fake_input_content:
        pass
    conn = sqlite_connection(get_db_path())

    unique_vids = execute_query(conn, "SELECT count(*) FROM videos "
                                      "WHERE NOT videos.id = 'unknown';")[0][0]
    total_opened = execute_query(conn, "SELECT count(*) FROM "
                                       "videos_timestamps;")[0][0]
    total_channels = execute_query(conn, "SELECT count(*) FROM channels;")[0][0]
    total_tags = execute_query(conn, "SELECT count(*) FROM videos_tags;")[0][0]
    unique_tags = execute_query(conn, "SELECT count(*) FROM tags;")[0][0]
    conn.close()
    mark_down = dcc.Markdown(dedent(f'''
    **{unique_vids}** unique videos 
    (and some with no identifying info available), opened/watched 
    **{total_opened}** times    
    **{total_tags}** tags across all videos (**{unique_tags}** unique), 
    an average of **{round(total_tags /unique_vids)}** per video    
    **{total_channels}** channels
    '''))

    return mark_down


def construct_history_chart(data: pd.DataFrame):
    data = [go.Scatter(x=data.watched_at, y=data.times,
                       mode='lines')]
    layout = go.Layout(
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
    data = history_chart.retrieve_watch_data(conn, date_periods_vals[value])
    conn.close()
    return construct_history_chart(data)


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
        summary_tables = history_chart.retrieve_data_for_a_date_period(
            conn, date)
        conn.close()
        return [*summary_tables]
    else:
        return history_date_period_summary_msg


def construct_v_scatter(df: pd.DataFrame,
                        x_axis_col: str, y_axis_col: str):

    layout = go.Layout(margin=dict.fromkeys(list('ltrb'), 40),
                       hovermode='closest',
                       yaxis=go.layout.YAxis(),
                       xaxis=go.layout.XAxis())
    if y_axis_col == 'Duration':
        duration_max = df.Duration.max()
        tick_vals = list(range(df.Duration.min(), duration_max + duration_max,
                               duration_max // 20))
        tick_text = []
        for v in tick_vals:
            if v >= 86400:
                tick_text.append(f'{v//86400}d')
            elif v >= 3600:
                tick_text.append(f'{v//3600}h')
            elif v >= 60:
                tick_text.append(f'{v//60}m')
            else:
                tick_text.append(f'{v}s')

        layout.yaxis.update(tickvals=tick_vals, ticktext=tick_text)
    # so the legend entries for channels with only one video are ordered in
    # descending order by views, for relevancy
    if len(df) >= 1000:
        buttons_to_remove = graph_tools_to_remove + ['zoom2d', 'pan2d',
                                                     'select2d', 'autoScale2d']
        layout.yaxis.update(fixedrange=True)
        layout.xaxis.update(fixedrange=True)
        data = [go.Scatter(x=df[x_axis_col], y=df[y_axis_col],
                           mode='markers',
                           hoverinfo='none',
                           marker={'opacity': 0.8, 'size': 6})]
    else:
        buttons_to_remove = graph_tools_to_remove
        channels = df.Channel.unique()

        channels_dict = {k: v for k, v
                         in zip(channels, list(range(len(channels))))}
        max_val = max(channels_dict.values())
        for i in channels_dict:
            channels_dict[i] /= max_val
        channels_colors = get_colors_from_colorscale(channels_dict)
        df = df.sort_values(by='Views', ascending=False)
        data = []
        for c in channels:
            c_df = df[df.Channel == c]
            c_name = c
            if len(c) > 20:
                c_name = c[:20] + '...'
            if len(c_df) == 1:
                legend_group = {'legendgroup': '1 record per Channel'}
                trace_name = f'{c_name}'
            else:
                legend_group = dict()
                trace_name = f'{c_name} ({len(c_df)})'
            data.append(go.Scatter(x=c_df[x_axis_col], y=c_df[y_axis_col],
                                   mode='markers',
                                   marker={'opacity': 0.8, 'size': 14,
                                           'color': channels_colors[c]},
                                   customdata=c_df.VideoID,
                                   name=trace_name,
                                   **legend_group,
                                   ))
        # so the legend list shows channels with most videos first
        data = sorted(data, key=lambda fig: len(fig.customdata), reverse=True)

    figure = go.Figure(data=data, layout=layout,)
    graph = dcc.Graph(
        id=v_scatter_id,
        figure=figure,
        config=dict(displaylogo=False,
                    modeBarButtonsToRemove=buttons_to_remove),
        style=dict(height=500, width=800)),
    return graph


@dash_app.callback(Output(v_scatter_container_id, 'children'),
                   [
                       Input(v_scatter_x_dropdown_id, 'value'),
                       Input(v_scatter_y_dropdown_id, 'value'),
                       Input(v_scatter_views_slider_id, 'value'),
                       Input(v_scatter_recs_id, 'value'),
                   ])
def update_v_scatter(x_axis_type: str, y_axis_type: str, views: list,
                     num_of_records: int):
    if not x_axis_type or not y_axis_type:
        return None
    num_of_records = v_scatter_slider_recs_nums[num_of_records]
    min_views = v_scatter_view_range_nums[views[0]]
    max_views = v_scatter_view_range_nums[views[1]]
    db_path = join(get_project_dir_path_from_cookie(), DB_NAME)
    conn = sqlite_connection(db_path)
    data = videos_scatter_graph.get_data(conn, x_axis_type, y_axis_type,
                                         min_views=min_views,
                                         max_views=max_views,
                                         number_of_records=num_of_records)
    conn.close()
    if x_axis_type in ['LikeRatioAsc', 'LikeRatioDesc']:
        x_axis_type = 'Ratio'
    return construct_v_scatter(data, x_axis_type, y_axis_type)


@dash_app.callback(Output(v_scatter_summary_id, 'children'),
                   [Input(v_scatter_id, 'hoverData')])
def v_scatter_summary(hover_data: dict):
    if hover_data:
        point_of_interest = hover_data['points'][0].get('customdata', None)
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
        views = add_commas_to_num(r[3])
        likes = add_commas_to_num(r[4]) if r[4] else None
        dislikes = add_commas_to_num(r[5]) if r[5] else None
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


@dash_app.callback(Output(tracking_table_container_id, 'children'),
                   [Input(tracking_choice_id, 'value')]
                   )
def top_watched_tracking(tracking_type):
    conn = sqlite_connection(get_db_path())
    results = tracking.get_top_results(conn, tracking_type)
    conn.close()
    return results


@dash_app.callback(Output(tracking_graph_id, 'figure'),
                   [Input('top-watched-table', 'selected_rows')],
                   [State(tracking_choice_id, 'value')]
                   )
def top_watched_tracking_graph(rows: list, query_type: str):
    entries: pd.DataFrame = getattr(tracking.data_keeper, query_type.lower())
    if query_type == 'Videos':
        ind = 2
    else:
        ind = 0
    entries: list = entries.iloc[rows, ind].values
    conn = sqlite_connection(get_db_path(), types=True)
    data = []

    df = tracking.selected_history_charts_mass(conn, entries, query_type)
    for value in entries:
        sub_df = df[df[df.columns[0]] == value]
        if query_type == 'Videos':
            name = sub_df.iloc[0, 1]
        else:
            name = value
        if len(name) > 20:
            name = name[:20] + '...'
        sub_df = sub_df.groupby(pd.Grouper(freq='MS'))
        sub_df = sub_df.size().reset_index(name='Views')
        data.append(go.Scatter(x=sub_df.Timestamp,
                               y=sub_df.Views,
                               mode='lines',
                               name=name
                               ))
    conn.close()
    layout = go.Layout(
        yaxis=go.layout.YAxis(fixedrange=True),
        margin=dict.fromkeys(list('ltrb'), 30),
        hovermode='closest'
    )

    return {'data': data, 'layout': layout}
