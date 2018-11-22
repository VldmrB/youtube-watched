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
    json_path = DESKTOP_PATH + r'\divs.json'
    with open(json_path, 'r') as file:
        data = json.load(file)
    prepped_data = {}
    date_range = [*range(start, end+1)]
    count = 0
    print(len(data['divs']))

    for i in data['divs']:
        if len(i) < 2:
            count += 1
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
    print(count)
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


def count_removed_in_original_file():
    path = r'D:\Downloads\takeout-20181120T163352Z-001\Takeout\YouTube\history'
    file = r'\watch-history.html'
    with open(path+file, 'r') as file:
        count = file.read().count('Watched a video that has been removed')
    print(count)


count_removed_in_original_file()
yt_data = load_data(start=2014, end=2018, period_length='month')
# plot_data(yt_data)
# Nov 19, 2018, 11:07:59 PM EST
# %b  %d,   %Y, %I:%M:%S %p  %Z
