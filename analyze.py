import os
import sqlite3
from collections import Counter

import dash_table
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from ktools import utils
from scipy.interpolate import interp1d

from sql_utils import execute_query
from testing import WORK_DIR

pd.set_option('display.max_columns', 400)
pd.set_option('display.width', 400)

gen_query = """SELECT v.title AS Title, c.title AS Channel,
    count(v.title) AS Views FROM 
    videos_timestamps vt JOIN videos v ON v.id = vt.video_id 
    JOIN channels c ON v.channel_id = c.id
    WHERE vt.watched_at LIKE ?
    GROUP BY v.id;"""
channels_query = """SELECT c.title AS Channel,
    count(c.title) AS Views FROM 
    videos_timestamps vt JOIN videos v ON v.id = vt.video_id 
    JOIN channels c ON v.channel_id = c.id
    WHERE vt.watched_at LIKE ?
    GROUP BY c.id
    ORDER BY Views desc;"""
tags_query = f"""SELECT t.tag AS Tag, count(t.tag) AS Amount FROM
    videos_timestamps vt JOIN videos_tags vtgs ON vtgs.video_id = vt.video_id 
    JOIN tags t ON vtgs.tag_id = t.id
    WHERE vt.watched_at LIKE ?
    GROUP BY t.tag
    ORDER BY Amount DESC
    LIMIT 10;"""
topics_query = f"""SELECT t.topic AS Topic, count(t.topic) AS Amount FROM
    videos_timestamps vt JOIN videos_topics v_topics
    ON vt.video_id = v_topics.video_id
    JOIN topics t ON v_topics.topic_id = t.id 
    WHERE vt.watched_at LIKE ?
    --AND t.topic NOT LIKE '%(parent topic)'
    GROUP BY t.topic
    ORDER BY Amount DESC
    LIMIT 10;"""

generic_table_settings = dict(
    merge_duplicate_headers=True,
    css=[{
        'selector': '.dash-cell div.dash-cell-value',
        'rule': 'display: inline; white-space: inherit;'
                'overflow: inherit; text-overflow: inherit;'
    }],
    style_header={'backgroundColor': 'rgb(31, 119, 180)',
                  'color': 'white'},
    style_cell={
        'whiteSpace': 'no-wrap',
        'overflow': 'hidden',
        'textOverflow': 'ellipsis',
        'maxWidth': 0,
    })
style_cell_cond_main = [
    {'if': {'column_id': 'Title'}, 'textAlign': 'left',
     'width': '600px', 'maxWidth': '600px', 'minWidth': '600px'
     },
    {'if': {'column_id': 'Channel'}, 'textAlign': 'left',
     'width': '600px', 'maxWidth': '600px', 'minWidth': '600px'
     },
    {'if': {'column_id': 'Channel/video'}, 'textAlign': 'left',
     'width': '600px', 'maxWidth': '600px', 'minWidth': '600px'
     },
    {'if': {'column_id': 'Views'},
     'width': '50px', 'maxWidth': '50px', 'minWidth': '50px'
     }
]
style_cell_cond_aux = [
    {'if': {'column_id': 'Tag'}, 'textAlign': 'left',
     'width': '200px', 'maxWidth': '200px', 'minWidth': '200px'
     },
    {'if': {'column_id': 'Topic'}, 'textAlign': 'left',
     'width': '200px', 'maxWidth': '200px', 'minWidth': '200px'
     },
    {'if': {'column_id': 'Amount'},
     'width': '50px', 'maxWidth': '50px', 'minWidth': '50px'
     }]


def refine(x: np.array, y: np.array, fine: int, kind: str = 'quadratic'):

    x_refined = np.linspace(x.min(), x.max(), fine)
    func = interp1d(x, y, kind=kind)
    y_refined = func(x_refined)
    return x_refined, y_refined


def retrieve_watch_data(conn: sqlite3.Connection) -> pd.DataFrame:
    query = 'SELECT watched_at FROM videos_timestamps'
    df = pd.read_sql(query, conn, index_col='watched_at')
    times = pd.Series(np.ones(len(df.index.values)))
    df = df.assign(times=times.values)

    df = df.groupby(pd.Grouper(freq='H')).aggregate(np.sum)
    full_df_range = pd.date_range(df.index[0], df.index[-1], freq='H')
    df = df.reindex(full_df_range, fill_value=0)
    df.index.name = 'watched_at'

    return df


def retrieve_data_for_a_date_period(conn: sqlite3.Connection, date: str):
    params = (date + '%',)

    tags = pd.read_sql(tags_query, conn, params=params)
    tags['Tag'] = tags['Tag'].str.lower()
    tags = tags.groupby(by='Tag').aggregate(list)
    tags['Amount'] = [sum(i) if not isinstance(i, int) else i
                      for i in tags['Amount']]

    tags = tags.sort_values(by='Amount', ascending=False)
    tags = tags.reset_index()
    topics = pd.read_sql(topics_query, conn, params=params)
    if len(date) > 7:
        if len(date) == 13:
            date = date + ':00'
        table_cols = [
            {'name': ['Channel/video'], 'id': 'Channel'},
            {'name': ['Views'], 'id': 'Views'}
        ]
        summary = pd.read_sql(gen_query, conn, params=params)
        channels = summary.groupby(by='Channel').aggregate(list)
        channels = channels.assign(Views=[
            sum(i) for i in channels.Views.values])
        channels = channels.sort_values(by='Views', ascending=False)
        channels = channels.drop(['Title'], axis=1)
        summary = summary.drop('Views', axis=1)
        summary = summary[summary['Title'] != 'unknown']
        summary = summary.sort_values(by='Channel')
        channels = channels.reset_index()
        channel_rows = channels.to_dict('rows')
        smmry_rows = summary.to_dict('rows')
        table_rows = []
        channel_rows_indexes = []
        for dct in channel_rows:
            table_rows.append(dct)
            channel_rows_indexes.append(len(table_rows)-1)
            for vid_dict in smmry_rows:
                if vid_dict['Channel'] == dct['Channel']:
                    row = {'Channel': vid_dict['Title']}
                    table_rows.append(row)
        style_cell_cond_main.extend(
            [{'if': {'row_index': i}, 'backgroundColor': '#A1C935'} for i
             in channel_rows_indexes])
    else:
        column_names = ['Channel', 'Views']
        table_cols = [{'name': [n], 'id': n}
                      for n in column_names]
        channels = pd.read_sql(channels_query, conn, params=params)
        table_rows = channels.to_dict('rows')

    views = (date + ' (total views: ' + str(channels.Views.sum()) + ')')
    for col_entry in table_cols:
        col_entry['name'].insert(0, views)
    main_table = dash_table.DataTable(
        columns=table_cols,
        data=table_rows, id='channels-table',
        style_table={'maxHeight': '400', 'maxWidth': '800'},
        n_fixed_rows=2,
        style_cell_conditional=style_cell_cond_main,
        **generic_table_settings)

    tags_cols = [{'name': n, 'id': n} for n in tags.columns]
    tags_rows = tags.to_dict('rows')
    tags_table = dash_table.DataTable(
        columns=tags_cols,
        data=tags_rows, id='tags-table',
        style_table={'maxHeight': '400',
                     'maxWidth': '300'},
        n_fixed_rows=1,
        style_cell_conditional=style_cell_cond_aux,
        **generic_table_settings)
    
    topics_cols = [{'name': n, 'id': n} for n in topics.columns]
    topics_rows = topics.to_dict('rows')
    topics_table = dash_table.DataTable(
        columns=topics_cols,
        data=topics_rows, id='topics-table',
        style_table={'maxHeight': '400',
                     'maxWidth': '300'},
        n_fixed_rows=1,
        style_cell_conditional=style_cell_cond_aux,
        **generic_table_settings)

    return main_table, tags_table, topics_table


def plot_data(data: pd.DataFrame, save_name=None):
    from matplotlib import dates

    x = data['watched_at']
    y = data['times']
    fig, ax = plt.subplots()
    fig_length = 0.3 * (len(x))
    fig.set_size_inches(fig_length, 5)
    fig.set_dpi(150)
    ax.plot(x, y, 'o-')
    ax.margins(fig_length*0.0008, 0.1)
    ax.xaxis.set_major_locator(dates.YearLocator())
    ax.xaxis.set_minor_locator(dates.MonthLocator(range(2, 13)))
    ax.xaxis.set_major_formatter(dates.DateFormatter('%m\n(%Y)'))
    ax.xaxis.set_minor_formatter(dates.DateFormatter('%m'))
    ax.set_title('Videos opened/watched over monthly periods')
    ax.grid(True, which='both', linewidth=0.1)
    plt.tight_layout()

    if save_name:
        plt.savefig(
            os.path.join(WORK_DIR, 'graphs', save_name),
            format='svg')
    plt.show()


@utils.timer
def plot_tags(conn: sqlite3.Connection):
    results = execute_query(conn,
                            '''SELECT t.tag FROM
                            tags t JOIN videos_tags vt ON t.id = vt.tag_id
                            ORDER BY t.tag;
                            ''')
    all_tags = Counter([i[0] for i in results])
    deduplicated_tags = {}

    for tag, tag_count in all_tags.items():
        all_tags[tag] = tag.lower()
        if tag == 'the':
            continue
        # for k, v in duplicate_tags.items():
        #     if tag in v:
        #         deduplicated_tags.setdefault(k, 0)
        #         deduplicated_tags[k] += tag_count
        #         break
        # else:
        deduplicated_tags.setdefault(tag, 0)
        deduplicated_tags[tag] += tag_count
    p_series = pd.Series(deduplicated_tags).sort_values()[-50:]
    # print(p_series.values)
    # p_series = p_series.filter(like='piano', axis=0)
    plt.figure(figsize=(5, 10))
    p_series.plot(kind='barh')
    # plt.barh(width=0.3, y=p_series.index)
    plt.show()
