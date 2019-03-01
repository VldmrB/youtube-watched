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


def retrieve_time_data(conn: sqlite3.Connection) -> pd.DataFrame:
    query = 'SELECT watched_at FROM videos_timestamps'
    df = pd.read_sql(query, conn, index_col='watched_at')
    times = pd.Series(np.ones(len(df.index.values)))
    df = df.assign(times=times.values)

    df = df.groupby(pd.Grouper(freq='H')).aggregate(np.sum)
    full_df_range = pd.date_range(df.index[0], df.index[-1], freq='H')
    df = df.reindex(full_df_range, fill_value=0)
    df.index.name = 'watched_at'

    return df


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
