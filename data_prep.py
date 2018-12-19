import os
import json
from datetime import datetime
from pprint import pprint
import utils
import ktools.dict_exploration
from convert_takeout import get_all_records
from config import WORK_DIR, video_keys_and_columns

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
watch_history = 'watch-history.json'


def get_data_and_basic_stats_from_takeout(silent=False):
    takeout_data_path = os.path.join(WORK_DIR, 'takeout_data')
    if os.path.exists(os.path.join(takeout_data_path, watch_history)):
        with open(takeout_data_path, 'r') as file:
            data = json.load(file)
    else:
        data = get_all_records(takeout_data_path, write_changes=True,
                               dump_json=True, silent=True)
    pure_data = []
    removed_videos_count, new_video_check, new_videos_without_urls = 0, 0, []
    for i in data['divs']:

        if len(i) < 2:
            removed_videos_count += 1
        if new_video_check < 300:
            for element in i:
                if utils.is_video_url(element):
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

    return pure_data


def get_time_data_from_takeout(start=2014, end=2018, period_length='year',
                               silent=True):
    takeout_data_path = os.path.join(WORK_DIR, 'takeout_data')
    with open(takeout_data_path, 'r') as file:
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
                if utils.is_video_url(element):
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


def get_videos_info_from_db(db_path, table_name: str):
    """Returns a list of records for videos which have not been deleted"""
    from ktools import db

    most_commonly_missing_tags = {}
    conn = db.sqlite_connection(db_path)
    cur = conn.cursor()
    no_tags_dict, removed_videos_count = {}, 0
    cur.execute(f"""SELECT * from {table_name} group by video_id""")
    all_records = cur.fetchall()
    good_records = []
    for row in all_records:
        try:
            assert json.loads(row[1])['items']
            jsonified = json.loads(row[1])['items'][0]
            good_records.append(jsonified)
            tags_missing = ktools.dict_exploration.get_missing_keys(
                jsonified, video_keys_and_columns)
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

    tags_always_found = [tag for tag in video_keys_and_columns if tag not
                         in most_commonly_missing_tags.keys()]

    print('Tags found in all the records:')
    pprint(tags_always_found)
    cur.close()
    conn.close()
    return good_records
