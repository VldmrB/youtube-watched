import json
import os
import sqlite3

from typing import Union
from ktools import db
from ktools import fs
from ktools.dict_exploration import get_final_key_paths

from config import DB_PATH, WORK_DIR, video_tags
from utils import convert_duration

logger = fs.logger_obj(os.path.join(WORK_DIR, 'logs', 'fail.log'))

main_table_cols = [
    'id text primary key',  # not using a separate integer as a PK
    'published_at timestamp',  # pass detect_types when creating connection
    'watched_at timestamp',
    'channel_id text',
    'title text',
    'description text',
    'category_id text',
    'default_audio_language text',
    'duration integer',  # needs conversion before passing
    'view_count integer',  # needs conversion before passing
    'like_count integer',  # needs conversion before passing
    'dislike_count integer',  # needs conversion before passing
    'comment_count integer',  # needs conversion before passing
    ('foreign key (channel_id) references channels (id) on update cascade on '
     'delete cascade'),
]

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


def execute_insert_query(conn: sqlite3.Connection,
                         query: str, values: Union[list, str] = None):
    cur = conn.cursor()
    if values is not None:
        values = tuple(values)
        cur.execute(query, values)
    else:
        cur.execute(query)
    conn.commit()
    conn.close()


def construct_videos_entry_from_json_obj(json_obj: dict):
    # todo finish this, currently setting up conversion for some values
    entry_dict = {}
    for path in get_final_key_paths(json_obj, '',
                                    True, black_list=['localized']):
        last_bracket = path[0].rfind('[\'')
        key = path[0][last_bracket+2:path[0].rfind('\'')]  # last key
        value = path[1]  # value of the above key
        if key in video_tags:
            new_key = ''
            for letter in key:
                if letter.isupper():
                    new_key += '_' + letter.lower()
                else:
                    new_key += letter
            key = new_key
            if key == 'relevant_topic_ids':
                value = list(set(value))  # duplicate topic ids
            elif key == 'duration':
                value = convert_duration(value)
            elif key == 'published_at':
                value = value.replace('T', ' ')[:value.find('.')]
            elif key in ['view_count', 'dislike_count', 'like_count',
                         'comment_count']:
                value = int(value)
            entry_dict[key] = value

    return entry_dict


def repopulate_videos_info_properly():
    import data_prep
    from topics import children_topics
    takeout = data_prep.get_data_and_basic_stats_from_takeout(silent=True)
    takeout_found = []
    js = [construct_videos_entry_from_json_obj(record) for record in
          data_prep.get_videos_info_from_db()]
    count = 0
    matches = 0
    decl_types = sqlite3.PARSE_DECLTYPES
    decl_colnames = sqlite3.PARSE_COLNAMES
    conn = db.sqlite_connection(DB_PATH,
                                detect_types=decl_types | decl_colnames)
    cur = conn.cursor()
    cur.close()
    inserted_tags = []
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
                js_row = js[row]
                if compare_to == 'id':
                    comparison = (js_row['id'], video_id)
                else:
                    comparison = (js_row['title'], video_id)
                do_compare = comparison[0] == comparison[1]
                if do_compare:
                    matches += 1
                    js_row['watched_on'] = takeout[takeout_row][-1]
                    takeout_found.append(takeout[takeout_row])
                    used = True
                else:
                    used = False

                channel_title = js_row.pop('channel_title')
                cur = conn.cursor()
                cur.execute(
                    """INSERT INTO channels (id, title)
                    values (?, ?)""", (js_row['id'], channel_title))
                conn.commit()
                cur.close()

                if 'tags' in js_row:
                    tags = js_row.pop('tags')
                else:
                    tags = None
                if 'topics' in js_row:
                    topics = js_row.pop('relevant_topic_ids')
                else:
                    topics = None

                question_marks = ', '.join(
                    ['?' for i in range(len(js_row.keys()))])
                print(question_marks, question_marks.count('?'))
                execute_insert_query(
                    conn,
                    ('INSERT INTO videos (\n' +
                     ', '.join([f'\'{key}\'' for key in js_row.keys()]) +
                     '\n)\nvalues (\n' + question_marks + '\n);'),
                    values=[*js_row.values()])

                if tags:
                    for tag in tags:
                        if tag not in inserted_tags:
                            execute_insert_query(
                                conn,
                                "INSERT INTO tags (tag) values (?)", tag)
                            inserted_tags.append(tag)
                        cur = conn.cursor()
                        cur.execute("SELECT id FROM tags WHERE tag = ?",
                                    (tag,))
                        tag_id = cur.fetchone()[0]

                        execute_insert_query(
                            conn,
                            """INSERT INTO videos_tags (video_id, tag_id) 
                            values (?, ?)""",
                            [js_row['id'], tag_id])

                if topics:
                    for topic in topics:
                        if topic in children_topics:
                            cur = conn.cursor()
                            cur.execute("""INSERT INTO videos_topics 
                            (video_id, topic_id) 
                            values (?, ?)""", (js_row['id'], topic))

                if used:
                    # pprint(js_row.keys())
                    del js[row]
                    raise SystemExit
                    # break

    takeout_found = [row[0] for row in takeout_found]
    takeout_orphans = [row for row in takeout
                       if len(row) > 2 and row[0] not in takeout_found]
    print('Unmatched from takeout:', len(takeout_orphans))
    print('Unmatched from js:', len(js))
    print('Total matches:', matches)


def time_test():
    conn = db.sqlite_connection(':memory:',
                                detect_types=sqlite3.PARSE_DECLTYPES)
    cur = conn.cursor()
    from datetime import datetime
    cur.execute('create table b (a timestamp)')
    cur.execute('insert into b values (?)', (datetime.now(),))
    cur.execute('select a from b')
    conn.commit()
    fetchone_ = cur.fetchone()[0]
    print(fetchone_)
    print(type(fetchone_))


def retrieve_test():
    conn = sqlite3.connect(':memory:')
    cur = conn.cursor()
    cur.execute('create table b (a int)')
    cur.execute('insert into b values (?)', (1,))
    cur.execute('select a from b')
    conn.commit()
    fetchone_ = cur.fetchone()[0]
    print(fetchone_)
    print(type(fetchone_))


if __name__ == '__main__':
    from os.path import join
    with open(join(WORK_DIR, 'video_info.json'), 'r') as js_file:
        js_file = json.load(js_file)

    # trial = construct_videos_entry_from_json_obj(js_file)
    # trial.pop('tags')
    # trial.pop('relevant_topic_ids')
    # trial.pop('description')
    # print('INSERT INTO videos (\n' +
    #       ', '.join([f'\'{key}\'' for key in trial.keys()]) +
    #       '\n)\nvalues (\n' + ', '.join([f'\'{val}\''
    #                                      for val in trial.values()]) + '\n);')
    time_test()
    # repopulate_videos_info_properly()
