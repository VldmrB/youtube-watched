import os
import json
from datetime import datetime
from pprint import pprint
import utils
from config import WORK_DIR, DB_PATH

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

video_tags = ['id', 'publishedAt',
              'channelId', 'title',
              'description',
              'channelTitle', 'tags',
              'categoryId',
              'defaultAudioLanguage',
              'duration',
              'viewCount',
              'likeCount',
              'dislikeCount',
              'commentCount',
              'relevantTopicIds']

main_table_cols = [
    'id text',
    'published_at timestamp',  # pass detect_types when creating connection
    'channel_id text',
    'title text',
    'description text',
    'channel_title text',
    'category_id text',
    'default_audio_language text',
    'duration integer',  # needs conversion before passing
    'view_count integer',  # needs conversion before passing
    'like_count integer',  # needs conversion before passing
    'dislike_count integer',  # needs conversion before passing
    'comment_count integer'  # needs conversion before passing
]
# print('CREATE TABLE youtube_videos_info_2 (' + ',\n'.join(main_table_cols)
#       + ');')

"""
Channel table
'channel_id text',
'channel_title text',

Video table

'id text',
'published_at timestamp',  # pass detect_types when creating connection
'title text',
'description text',
'category_id text',
'default_audio_language text',  # not always present
'duration integer',  # convert?
'view_count integer',  # convert; not always present
'like_count integer',  # convert; not always present
'dislike_count integer',  # convert; not always present
'comment_count integer'  # convert; not always present
'watched_on timestamp'  # added from takeout data where present

Tags table
'tag text'

Topics and subtopics' tables
'relevant_topic_id'  # not always present
"""


def get_data_and_basic_stats_from_takeout(silent=False):
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
                if utils.check_if_url(element):
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
                if utils.check_if_url(element):
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


def get_videos_info_from_db():
    """Returns a list of records for videos which have not been deleted"""
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
            tags_missing = utils.get_missing_keys(
                jsonified, video_tags)
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

    tags_always_found = [tag for tag in video_tags if tag not
                         in most_commonly_missing_tags.keys()]

    print('Tags found in all the records:')
    pprint(tags_always_found)
    cur.close()
    conn.close()
    return good_records


get_videos_info_from_db()
