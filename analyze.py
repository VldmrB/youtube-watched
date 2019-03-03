import os
import sqlite3
from collections import Counter
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.interpolate import interp1d
from ktools import utils
from testing import WORK_DIR
from sql_utils import execute_query
# import plotly.graph_objs as go
# import dash_core_components as dcc


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


def retrieve_data_for_a_date_period(conn: sqlite3.Connection,
                                    date: str) -> pd.DataFrame:
    pd.set_option('display.max_columns', 1000)
    pd.set_option('display.width', 10000)
    gen_query = """SELECT v.title AS video_title, c.title AS channel_title,
    count (v.title) AS amount FROM 
    videos_timestamps vt JOIN videos v ON v.id = vt.video_id 
    JOIN channels c ON v.channel_id = c.id
    WHERE vt.watched_at LIKE ?
    GROUP BY v.id;"""
    tags_query = """SELECT t.tag AS tags, count(t.tag) AS amount FROM
    videos_timestamps vt JOIN videos_tags vtgs ON vtgs.video_id = vt.video_id 
    LEFT JOIN tags t ON vtgs.tag_id = t.id
    WHERE vt.watched_at LIKE ?
    GROUP BY t.tag
    ORDER BY count(t.tag) DESC
    LIMIT 10;"""
    topics_query = """SELECT t.topic AS topics, count(t.topic) AS amount FROM
    videos_timestamps vt JOIN videos_topics v_topics
    ON vt.video_id = v_topics.video_id
    JOIN topics t ON v_topics.topic_id = t.id 
    WHERE vt.watched_at LIKE ?
    GROUP BY t.topic
    ORDER BY count(t.topic) DESC
    LIMIT 5;"""

    date = (date + '%',)

    smmry = pd.read_sql(gen_query, conn, params=date)
    smmry = smmry.groupby(by=smmry['channel_title']).aggregate(np.array)
    smmry = smmry.assign(total_amount=[i.sum() for i in smmry.amount.values])
    smmry = smmry.sort_values(by='total_amount', ascending=False)

    tag_10 = pd.read_sql(tags_query, conn, params=date)
    tag_10['tags'] = tag_10['tags'].str.lower()
    tag_10 = tag_10.groupby(by=tag_10['tags']).aggregate(np.array)
    tag_10['amount'] = [i.sum() for i in tag_10['amount']
                        if not isinstance(i, int)]
    print(tag_10.head(10))
    return smmry


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


# def plotly_watch_chart(data: pd.DataFrame, date_interval='D'):
#     df = data.groupby(pd.Grouper(freq=date_interval)).aggregate(np.sum)
#     df = df.reset_index()
#     graph = dcc.Graph(id='le-graph',
#                       figure={
#                           'data': [
#                               go.Scatter(x=df.watched_at,
#                                          y=df.times,
#                                          mode='lines')]})
#     return graph
