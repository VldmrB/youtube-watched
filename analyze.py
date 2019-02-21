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
    # print(df.columns.values)
    # exit()

    return df


def plot_data(data: pd.DataFrame, save_path=None):
    import plotly
    from matplotlib import dates

    x = data.index
    y = data.values
    x_mid = x.to_pydatetime()
    x1 = [i for i in x_mid]
    y1 = [i[0] for i in y]
    fig, ax = plt.subplots()
    fig_length = 0.3 * (len(x))
    fig.set_size_inches(fig_length, 5)
    fig.set_dpi(150)
    ax.plot(x, y, 'o-')
    # ax.bar(x, [i[0] for i in y], width=8)
    ax.margins(fig_length*0.0008, 0.1)
    ax.xaxis.set_major_locator(dates.YearLocator())
    ax.xaxis.set_minor_locator(dates.MonthLocator(range(2, 13)))
    ax.xaxis.set_major_formatter(dates.DateFormatter('%m\n(%Y)'))
    ax.xaxis.set_minor_formatter(dates.DateFormatter('%m'))
    ax.set_title('Videos opened/watched over monthly periods')
    ax.grid(True, which='both', linewidth=0.1)
    plt.tight_layout()

    if save_path:
        plt.savefig(
            os.path.join(WORK_DIR, save_path),
            format='svg')
    plt.subplots_adjust(top=2.1, bottom=2, hspace=1)
    plt.show()
    wut = plotly.graph_objs.Bar(x=x1, y=y1)

    return plotly.offline.plot([wut], include_plotlyjs=False,
                               output_type='div')


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


def altair(data: pd.DataFrame):
    import altair as alt

    brush = alt.selection(type='interval', encodings=['x'])

    month_data = data.groupby(pd.Grouper(freq='MS')).aggregate(np.sum)
    month_chart = alt.Chart(month_data.reset_index(),
                            width=1800,
                            height=75,
                            title='Videos by month').mark_line().encode(
        alt.X('watched_at', axis=alt.Axis(title='',
                                          grid=False,
                                          ),
              ),
        alt.Y('times', axis=alt.Axis(title=''))).add_selection(brush)

    day_data = data.groupby(pd.Grouper(freq='D')).aggregate(np.sum)
    day_data = day_data.truncate(after=month_data.index[-1])
    day_chart = alt.Chart(day_data.reset_index(),
                          width=1800,
                          title='Videos by day').mark_line().encode(
        alt.X('watched_at', axis=alt.Axis(title=''),
              scale={'domain': brush.ref(), 'clamp': True}),
        alt.Y('times', axis=alt.Axis(title='')))

    layered_chart = alt.vconcat(day_chart, month_chart)

    layered_chart.save(r'G:\\test_dir_1\\alt.html')
    return layered_chart.to_dict()
