import os
import json
from calendar import month_abbr
from datetime import datetime
from pprint import pprint

import matplotlib.pyplot as plt
import numpy as np
from scipy.interpolate import interp1d

from config import DB_PATH, WORK_DIR


# Nov 19, 2018, 11:07:59 PM EST
# %b  %d,   %Y, %I:%M:%S %p  %Z


def refine(x: np.array, y: np.array, fine: int, kind: str='cubic'):
    x_refined = np.linspace(x.min(), x.max(), fine)
    func = interp1d(x, y, kind=kind)
    y_refined = func(x_refined)
    return x_refined, y_refined


def load_takeout_data(start=2014, end=2018, period_length='year', silent=True):
    json_path = os.path.join(WORK_DIR, 'divs.json')
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
    pprint(data)
    x_orig = np.array([i[0] for i in data])
    y_orig = np.array([i[1] for i in data])
    # raise SystemExit
    x, y = refine(x_orig, y_orig, len(x_orig)*20)
    plt.plot(x, y)
    plt.xticks([*range(1, len(x_orig)+1)], month_abbr[1:])
    plt.show()
    # plt.imsave()


def sort_by_watched_type():
    """Counts by 'watched', 'watched a video that has been removed', etc."""
    json_path = os.path.join(WORK_DIR, 'original_divs.json')
    with open(json_path, 'r') as file:
        data = json.load(file)
    story_count, removed_count, prepped_data = 0, 0, {}

    for div in data['divs']:
        from_ = div[1].split('ory')[0]
        prepped_data.setdefault(from_, 0)
        prepped_data[from_] += 1
        if 'Watched story' in div[1]:
            story_count += 1
        elif 'removed' in div[1]:
            removed_count += 1
    pprint(prepped_data)
    print('Stories watched:', story_count)
    print('Videos removed:', removed_count)
    print('Total video records:', len(data['divs']))


def plot_tags():
    from ktools import db

    all_tags = {}
    conn = db.sqlite_connection(DB_PATH)
    cur = conn.cursor()
    no_tags_dict, removed_videos_count = {}, 0
    cur.execute("""SELECT * from youtube_videos_info;""")
    while True:
        fetchone = cur.fetchone()
        if not fetchone:
            break
        try:
            json_str = json.loads(fetchone[1])['items'][0]['snippet']['tags']
            for tag in json_str:
                all_tags.setdefault(tag, 0)
                all_tags[tag] += 1
        except KeyError:
            # no tags key for whatever reason
            json_str = json.loads(fetchone[1])
            no_tags_dict[json_str['items'][0]['id']] = json_str[
                'items'][0]['snippet']['title']
        except IndexError:
            # video has been removed
            removed_videos_count += 1
    all_tags = sorted([[k, v] for k, v in all_tags.items()],
                      key=lambda entry: entry[1])

    cur.close()
    conn.close()

    # pprint(all_tags[-100:])
    print('No tags:', len(no_tags_dict))
    pprint(no_tags_dict)
    print('Removed videos:', removed_videos_count)


# plot_tags()
# plot_data(load_takeout_data(2015, 2017, period_length='month', silent=False))
