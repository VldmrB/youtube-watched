import os
import json
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.interpolate import interp1d
from ktools import utils
from testing import WORK_DIR
from data_prep import get_videos_info_from_db, duplicate_tags

# Nov 19, 2018, 11:07:59 PM EST
# %b  %d,   %Y, %I:%M:%S %p  %Z
# https://www.youtube.com/watch?v=Pd3P8L-EIAo


def refine(x: np.array, y: np.array, fine: int, kind: str='quadratic'):
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
def plot_tags():
    records = get_videos_info_from_db()
    all_tags = {}
    no_tags_dict = {}
    for record in records:
        try:
            json_str = json.loads(record[1])['items'][0]['snippet']['tags']
            for tag in json_str:
                tag = tag.lower()
                if tag == 'the':
                    continue
                for k, v in duplicate_tags.items():
                    if tag in v:
                        all_tags.setdefault(k, 0)
                        all_tags[tag] += 1
                        break
                else:
                    all_tags.setdefault(tag, 0)
                    all_tags[tag] += 1
        except KeyError:
            # no tags key for whatever reason
            json_str = json.loads(record[1])
            no_tags_dict[json_str['items'][0]['id']] = json_str[
                'items'][0]['snippet']['title']
    p_series = pd.Series(all_tags).sort_values()[-50:]
    # p_series = p_series[p_series > 25]
    # p_series = p_series.filter(like='gam', axis=0)
    p_series.plot(kind='barh')
    plt.show()
