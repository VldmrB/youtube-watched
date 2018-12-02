import os
import json
from datetime import datetime
from pprint import pprint
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.interpolate import interp1d

from ktools import utils
from config import DB_PATH, WORK_DIR


# Nov 19, 2018, 11:07:59 PM EST
# %b  %d,   %Y, %I:%M:%S %p  %Z
# https://www.youtube.com/watch?v=Pd3P8L-EIAo

duplicate_tags = {
    'pewdiepie': ['pdp', 'pewds', 'pewdiepie', 'pewdie'],
    'walkthrough': ['walkthrough', 'playthrough'],
    'ark': ['ark', 'Ark', 'taming', 'ark survival evolved', 'ark gameplay'],
    'skyrim': ['skyrim', 'skyrim mods'],
    'ygs': ['ygs', 'your grammar sucks', 'jacksfilms', 'jack douglass',
            'the best of your grammer sucks', 'ygs 50', 'ygs 100'],
    'game': ['game', 'games', 'gaming', 'game', 'video game (industry)',
             'gameplay'],
    'dog': ['dog', 'dogs'],
    'neebs': ['neebs', 'neebs gaming'],
    'wow': ['world of warcraft', 'wow', 'world'],

}

good_tags = ['id', 'publishedAt',
             'channelId', 'title',
             'description',
             'channelTitle', 'tags',
             'categoryId',
             'defaultAudioLanguage',
             'duration', 'dimension',
             'viewCount',
             'likeCount',
             'dislikeCount',
             'commentCount',
             'relevantTopicIds',
             'topicCategories']


def check_for_missing_final_keys(dict_to_check: dict):
    count = 0
    final_keys = []

    def recurse(dict_: dict):
        """Returns keys whose values are not dictionaries"""
        # print(dict_.keys())
        for i in dict_.keys():
            if isinstance(dict_[i], dict):
                recurse(dict_[i])
            else:
                final_keys.append(i)
                nonlocal count
                count += 1

    recurse(dict_to_check)
    # print('Total keys with non-dict values:', count)
    # pprint(final_keys, compact=True, width=25)

    tags_missing = [tag for tag in good_tags if tag not in final_keys]

    # print('Missing tags:', tags_missing)
    return tags_missing


def refine(x: np.array, y: np.array, fine: int, kind: str='quadratic'):
    x_refined = np.linspace(x.min(), x.max(), fine)
    func = interp1d(x, y, kind=kind)
    y_refined = func(x_refined)
    return x_refined, y_refined


def check_if_url(element): return True if 'youtube.com' in element else False


def get_data_and_basic_stats(silent=False):
    json_path = os.path.join(WORK_DIR, 'divs.json')
    with open(json_path, 'r') as file:
        data = json.load(file)
    pure_data = []
    removed_videos_count, new_video_check, new_videos_without_urls = 0, 0, []
    for i in data['divs']:

        if len(i) < 2:
            removed_videos_count += 1
        if new_video_check < 300:
            for element in i:
                if check_if_url(element):
                    new_videos_without_urls.append(i)
            new_video_check += 1
        index = datetime.strptime(i[-1][:-4], '%b %d, %Y, %I:%M:%S %p')
        appendee = []
        if len(i) == 3:
            appendee.extend([i[0], i[1], index])
        elif len(i) == 2:
            appendee.extend([i[0], index])
        elif len(i) == 1:
            # video has been removed
            appendee.append(index)
        pure_data.append(appendee)
    if not silent:
        print('-'*50)
        print('Total videos opened/watched:', len(data['divs']))
        print('Videos removed:', removed_videos_count)
        # print(f'Videos with urls in the top {new_video_check}:'
        #       f' {len(new_videos_without_urls)}:')
        # pprint(new_videos_without_urls)

    return pure_data


def get_time_data(start=2014, end=2018, period_length='year', silent=True):
    json_path = os.path.join(WORK_DIR, 'divs.json')
    with open(json_path, 'r') as file:
        data = json.load(file)
    prepped_data = {}
    date_range = [*range(start, end+1)]
    removed_videos_count = 0
    new_video_check, new_videos_without_urls = 0, []
    for i in data['divs']:

        if len(i) < 2:
            removed_videos_count += 1
        if new_video_check < 300:
            for element in i:
                if check_if_url(element):
                    new_videos_without_urls.append(i)
            new_video_check += 1
        index = datetime.strptime(i[-1][:-4], '%b %d, %Y, %I:%M:%S %p')
        if index.year not in date_range:
            continue
        if period_length == 'year':
            key = index.year
        else:
            key = index.month
        prepped_data.setdefault(key, 0)
        prepped_data[key] += 1
    if not silent:
        # for k, v in prepped_data.items():
        #     print(f'|||||||{k}:||||||')
        #     pprint(v)
        print('-'*50)
        print('Total videos opened/watched:', len(data['divs']))
        print('Videos removed:', removed_videos_count)
        print(f'Videos with urls in the top {new_video_check}:'
              f' {len(new_videos_without_urls)}:')
        pprint(new_videos_without_urls)
    return prepped_data


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


def get_videos_info():
    from ktools import db

    most_commonly_missing_tags = {}
    conn = db.sqlite_connection(DB_PATH)
    cur = conn.cursor()
    no_tags_dict, removed_videos_count = {}, 0
    cur.execute("""SELECT * from youtube_videos_info;""")
    all_records = cur.fetchall()
    good_records = []
    for row in all_records:
        try:
            assert json.loads(row[1])['items']
            jsonified = json.loads(row[1])['items'][0]
            good_records.append(row)
            tags_missing = check_for_missing_final_keys(jsonified)
            for tag in tags_missing:
                most_commonly_missing_tags.setdefault(tag, 0)
                most_commonly_missing_tags[tag] += 1

        except AssertionError:
            # video has been removed
            removed_videos_count += 1
            # print(row[1])

    print(f'Missing tags from {len(good_records)}:')
    pprint(
        sorted(list(
            most_commonly_missing_tags.items()),
                       key=lambda entry: entry[1], reverse=True))

    tags_always_found = [tag for tag in good_tags if tag not
                         in most_commonly_missing_tags.keys()]

    print('Tags found in all the records:')
    pprint(tags_always_found)
    cur.close()
    conn.close()
    return good_records


@utils.timer
def plot_tags():
    records = get_videos_info()
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
                    if tag in v:  # TODO continue here
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

    # print('No tags:', len(no_tags_dict))


def add_datetimes_where_possible():
    takeout = get_data_and_basic_stats(silent=True)
    takeout_found = []
    js = [list(row) for row in get_videos_info()]
    for row in range(len(js)):
        js[row][1] = json.loads(js[row][1])['items'][0]['snippet']['title']
    count = 0
    matches = 0
    for takeout_row in range(len(takeout)):
        if len(takeout[takeout_row]) > 1:
            count += 1
            strip = takeout[takeout_row][0].strip()
            if 'https://www.youtube.com/watch?v=' in strip:
                equal_index = strip.find('=')
                video_id = strip[equal_index+1:]
                duration_index = video_id.find('&t=')
                if duration_index > 0:
                    video_id = video_id[:duration_index]
                compare_to = 'id'
            else:
                video_id = strip
                compare_to = 'title'
            for row in range(len(js)):
                if compare_to == 'id':
                    comparison = (js[row][0], video_id)
                else:
                    comparison = (js[row][1], video_id)
                do_compare = comparison[0] == comparison[1]
                if do_compare:
                    matches += 1
                    del js[row]
                    takeout_found.append(takeout[takeout_row])
                    break

    takeout_found = [row[0] for row in takeout_found]
    takeout_orphans = [row for row in takeout
                       if len(row) > 2 and row[0] not in takeout_found]
    print('Unmatched from takeout:', len(takeout_orphans))
    print('Unmatched from js:', len(js))
    print('Total matches:', matches)


def fun_func():
    import sqlite3
    import datetime
    import time

    def adapt_datetime(ts):
        return time.mktime(ts.timetuple())

    def retrieve_datetime(dt):
        return datetime.datetime.fromtimestamp(float(dt))
    sqlite3.register_adapter(datetime.datetime, adapt_datetime)
    sqlite3.register_converter('time', retrieve_datetime)
    con = sqlite3.connect(":memory:", detect_types=sqlite3.PARSE_COLNAMES)
    cur = con.cursor()

    now = datetime.datetime.now()
    # cur.execute("select ?", (now,))
    cur.execute("""create table test_one (time int);""")
    con.commit()
    cur.execute("""insert into test_one values (?)""", (now,))
    con.commit()
    cur.execute("""select time as "time [time]" from test_one""")
    print(cur.fetchone()[0])


get_videos_info()
