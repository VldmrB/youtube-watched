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

    df = df.groupby(pd.Grouper(freq='H')).aggregate(np.sum)
    full_df_range = pd.date_range(df.index[0], df.index[-1], freq='H')
    df = df.reindex(full_df_range, fill_value=0)
    df.index.name = 'watched_at'

    return df


def plotly_try(data: pd.DataFrame, save_path: str = None):
    import plotly
    from matplotlib import dates
    data = data.reset_index()
    x = data['watched_at']
    y = data['times']

    fig, ax = plt.subplots()
    ax.plot(x, y)
    ax.xaxis.set_major_locator(dates.YearLocator())
    ax.xaxis.set_minor_locator(dates.MonthLocator(range(2, 13)))
    ax.xaxis.set_major_formatter(dates.DateFormatter('%m\n(%Y)'))
    ax.xaxis.set_minor_formatter(dates.DateFormatter('%m'))
    ax.set_title('Videos opened/watched over monthly periods')
    ax.grid(True, which='both', linewidth=0.1)
    plt.tight_layout()

    layout = plotly.graph_objs.Layout(xaxis=dict(type='date', autorange=True),
                                      yaxis=dict(type='log'))
    finished_plot = plotly.graph_objs.Scatter(x=x, y=y, mode='lines')
    fig = plotly.graph_objs.Figure(data=[finished_plot], layout=layout)

    plotly.offline.plot(fig, filename=save_path)


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


def altair_line_highlight(data: pd.DataFrame):
    # from datetime import datetime
    import altair as alt

    data = data.groupby(pd.Grouper(freq='D')).aggregate(np.sum)
    full_data_range = pd.date_range(data.index[0], data.index[-1], freq='D')
    data = data.reindex(full_data_range, fill_value=0)
    data.index.name = 'watched_at'

    brush = alt.selection(type='interval', encodings=['x'])
    # month_data: pd.DataFrame
    month_data = data.groupby(pd.Grouper(freq='MS')).aggregate(np.sum)
    month_data = month_data.reset_index()
    month_data.at[0, 'watched_at'] = data.index[0]
    month_chart = alt.Chart(month_data,
                            height=100,
                            title='Videos by month').mark_line().encode(
        alt.X('watched_at', axis=alt.Axis(title='')),
        alt.Y('times', axis=alt.Axis(title=''))).add_selection(brush)

    single = alt.selection(type='single', fields=['watched_at'], nearest=True,
                           empty='none', on='mouseover')

    day_data = data.reset_index()
    # day_data: pd.DataFrame
    # day_data['watched_at'] = day_data['watched_at'].apply(
    #     lambda x: datetime.strftime(x, '%Y-%m-%d %H:%M:%S'))
    day_chart = alt.Chart(day_data,
                          height=100,
                          title='Videos by day').mark_line(strokeWidth=1,
        point='transparent').encode(
        alt.X('watched_at', axis=alt.Axis(title=''),
              scale={'domain': brush.ref()}),
        alt.Y('times', axis=alt.Axis(title=''),
              scale=alt.Scale(domain=[0, day_data['times'].max()+5])),
        tooltip=[
            alt.TextFieldDefWithCondition(type='temporal', field='watched_at',
                                          format='%b %d, %Y', title='date'),
            alt.TextFieldDefWithCondition(type='quantitative',
                                          field='times', title='videos'),
            ])

    selectors = alt.Chart().mark_point().encode(
        x='watched_at:T', opacity=alt.value(0)).add_selection(single)

    text = day_chart.mark_text(align='left', dx=0, dy=-25).encode(
        text=alt.condition(single, 'watched_at', alt.value(' ')))

    layered_chart = alt.vconcat(
        alt.layer(day_chart, selectors, text), month_chart)
    # layered_chart.save(os.path.join(WORK_DIR, 'graphs', 'alt.svg'),
    #                    format='svg')
    return layered_chart.to_dict()

