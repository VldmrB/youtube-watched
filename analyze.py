import os
import sqlite3
from collections import Counter
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.interpolate import interp1d
from ktools import utils
from testing import WORK_DIR
from data_prep import duplicate_tags
from sql_utils import execute_query


def refine(x: np.array, y: np.array, fine: int, kind: str = 'quadratic'):
    x_refined = np.linspace(x.min(), x.max(), fine)
    func = interp1d(x, y, kind=kind)
    y_refined = func(x_refined)
    return x_refined, y_refined


def plot_data(data: dict, save_path=None):

    data = sorted([[k, v] for k, v in data.items()], key=lambda entry: entry[0])
    x_ticks = [i[0] for i in data]
    x_orig = np.array(x_ticks)
    y_orig = np.array([i[1] for i in data])
    x, y = refine(x_orig, y_orig, len(x_orig)*20)
    plt.plot(x, y)
    plt.xticks([*range(x_ticks[0], x_ticks[-1]+1)])
    if save_path:
        plt.savefig(os.path.join(WORK_DIR, save_path))
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
        for k, v in duplicate_tags.items():
            if tag in v:
                deduplicated_tags.setdefault(k, 0)
                deduplicated_tags[k] += tag_count
                break
        else:
            deduplicated_tags.setdefault(tag, 0)
            deduplicated_tags[tag] += tag_count
    p_series = pd.Series(deduplicated_tags).sort_values()
    p_series = p_series.filter(like='piano', axis=0)
    p_series.plot(kind='barh')
    plt.show()
