import numpy as np
import matplotlib.pyplot as plt
import json
from datetime import datetime
from pprint import pprint
from scipy.interpolate import interp1d
from calendar import month_abbr
from ktools.env import *


def refine(x: np.array, y: np.array, fine: int, kind: str='cubic'):
    x_refined = np.linspace(x.min(), x.max(), fine)
    func = interp1d(x, y, kind=kind)
    y_refined = func(x_refined)
    return x_refined, y_refined


def load_data(start=2014, end=2018, period_length='year', silent=True):
    json_path = G_PYTON_PATH + r'\youtube_watched_data\divs.json'
    with open(json_path, 'r') as file:
        data = json.load(file)
    prepped_data = {}
    date_range = [*range(start, end+1)]
    removed_videos_count = 0

    for i in data['divs']:
        if len(i) < 2:
            removed_videos_count += 1
        index = datetime.strptime(i[-1][:-4], '%b %d, %Y, %I:%M:%S %p')
        if index.year not in date_range:
            continue
        # appendee = []
        # if len(data) == 3:
        #     appendee.extend([i[0], i[1], index])
        # elif len(data) == 2:
        #     appendee.extend([i[0], index])
        # else:
        #     appendee.append(index)

        if period_length == 'year':
            key = index.year
        else:
            key = index.month
        prepped_data.setdefault(key, 0)
        prepped_data[key] += 1
    if not silent:
        for k, v in prepped_data.items():
            print(f'|||||||{k}:||||||')
            pprint(v)
        print('-'*50)
        print('Total videos opened/watched:', len(data['divs']))
        print('Videos removed:', removed_videos_count)
    return prepped_data


def plot_data(data: dict):

    data = sorted([[k, v] for k, v in data.items()], key=lambda entry: entry[0])
    x_orig = np.array([i[0] for i in data])
    y_orig = np.array([i[1] for i in data])
    x, y = refine(x_orig, y_orig, 250)
    plt.plot(x, y)
    plt.xticks([*range(1, 13)], month_abbr[1:])
    plt.show()
    plt.imsave()
    pprint(data)


def load_full_data(start=2014, end=2018):
    json_path = G_PYTON_PATH + r'\youtube_watched_data\original_divs.json'
    with open(json_path, 'r') as file:
        data = json.load(file)
    story_count = 0
    removed_count = 0
    prepped_data = {}
    for div in data['divs']:
        from_ = div[1].split('from')[0]
        prepped_data.setdefault(from_, 0)
        prepped_data[from_] += 1
        if 'Watched story from' in div[1]:
            # print(div[1:])
            story_count += 1
        elif 'removed' in div[1]:
            removed_count += 1
    pprint(prepped_data)
    print('Stories watched:', story_count)
    print('Videos removed:', removed_count)


def count_removed_in_original_file():
    path = r'D:\Downloads\takeout-20181120T163352Z-001\Takeout\YouTube\history'
    file = r'\watch-history.html'
    with open(path+file, 'r') as file:
        count = file.read().count('Watched a video that has been removed')
    print(count)


# count_removed_in_original_file()
# yt_data = load_data(start=2017, end=2017, period_length='month', silent=False)
load_full_data()
# plot_data(yt_data)
# Nov 19, 2018, 11:07:59 PM EST
# %b  %d,   %Y, %I:%M:%S %p  %Z
