import os
import sqlite3

from typing import Union, Optional
from ktools import db
from ktools import fs
from ktools.dict_exploration import get_final_key_paths
from ktools.utils import err_display, timer

from config import DB_PATH, WORK_DIR, video_keys_and_columns
from utils import convert_duration

logger = fs.logger_obj(os.path.join(WORK_DIR, 'logs', 'fail_populate.log'))

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


def generate_insert_query(table: str,
                          columns_values: Optional[dict] = None,
                          columns: Optional[Union[list, tuple]] = None)-> str:
    """
    Constructs a basic insert query.
    Requires only one of the optional kwargs to be filled.
    :param table:
    :param columns_values:
    :param columns:
    :return:
    """

    if not columns_values and not columns:
        raise ValueError('Nothing was passed.')

    if columns_values:
        columns = columns_values.keys()

    val_amount = len(columns)
    values_placeholders_str = '(' + ('?, ' * val_amount).strip(' ,') + ')'
    columns_str = '(' + ', '.join(columns) + ')'

    return f'INSERT INTO {table} {columns_str} VALUES {values_placeholders_str}'


def execute_insert_query(conn: sqlite3.Connection,
                         query: str, values: Union[list, str, tuple] = None):
    cur = conn.cursor()
    try:
        if values is not None:
            if isinstance(values, list):
                values = tuple(values)
            elif isinstance(values, str):
                values = (values,)

            cur.execute(query, values)
        else:
            cur.execute(query)
        conn.commit()
        cur.close()

        return True
    except sqlite3.Error as e:
        if values:
            values = f'{list(values)}'
        logger.error(f'Error: {e}\n'
                     f'query = \'{query}\'\n'
                     f'values = {values}')

    finally:
        cur.close()


def wrangle_video_record_for_sql(json_obj: dict):
    entry_dict = {}
    for key_value_pair in get_final_key_paths(
            json_obj, '', True, black_list=['localized']):
        last_bracket = key_value_pair[0].rfind('[\'')
        key = key_value_pair[0][last_bracket+2:key_value_pair[0].rfind('\'')]
        # ^ last key in each path
        value = key_value_pair[1]  # value of the above key
        if key in video_keys_and_columns:  # converting camelCase to underscore
            new_key = []
            for letter in key:
                if letter.isupper():
                    new_key.append('_' + letter.lower())
                else:
                    new_key.append(letter)
            key = ''.join(new_key)
            if key == 'relevant_topic_ids':
                value = list(set(value))  # due to duplicate topic ids
            elif key == 'duration':
                value = convert_duration(value)
            elif key == 'published_at':
                value = value.replace('T', ' ')[:value.find('.')]
            elif key in ['view_count', 'dislike_count', 'like_count',
                         'comment_count']:
                value = int(value)
            entry_dict[key] = value

    return entry_dict


@timer
def repopulate_videos_info_properly():
    import data_prep
    from topics import children_topics
    # from pprint import pprint
    videos_inserted = []
    channels_inserted = []
    tags_inserted = []
    js = [wrangle_video_record_for_sql(record) for record in
          data_prep.get_videos_info_from_db('temp_test')]

    sql_fails = 0
    success_count = 0
    decl_types = sqlite3.PARSE_DECLTYPES
    decl_colnames = sqlite3.PARSE_COLNAMES
    conn = db.sqlite_connection(DB_PATH,
                                detect_types=decl_types | decl_colnames)
    cur = conn.cursor()
    cur.execute("""SELECT id FROM videos;""")
    videos_ids = [row[0] for row in cur.fetchall()]
    cur.close()
    cur = conn.cursor()
    cur.execute("""SELECT id FROM channels;""")
    channels = [row[0] for row in cur.fetchall()]
    cur.close()
    cur = conn.cursor()
    cur.execute("""SELECT tag FROM tags;""")
    existing_tags = [row[0] for row in cur.fetchall()]
    cur.close()
    logger.info(f'Connected to DB: {DB_PATH}'
                f'\nStarting records\' insertion...\n' + '-'*150)

    for js_row in js:
        success = True
        # everything that doesn't go into the videos table gets popped
        # so the video table insert query can be constructed
        # automatically with the remaining items, since their amount
        # will vary from row to row
        if js_row['id'] in videos_ids:
            continue
        channel_id = js_row['channel_id']
        if channel_id not in channels:
            if 'channel_title' in js_row:
                channel_title = js_row.pop('channel_title')
                insert_result = execute_insert_query(
                    conn,
                    """INSERT INTO channels (id, title)
                    values (?, ?)""", (js_row['channel_id'], channel_title))
            else:
                insert_result = execute_insert_query(
                    conn,
                    """INSERT INTO channels (id)
                    values (?)""", (js_row['channel_id']))
            if not insert_result:
                sql_fails += 1
            else:
                channels_inserted.append(js_row['channel_id'])

        if 'tags' in js_row:
            tags = js_row.pop('tags')
        else:
            tags = None
        if 'relevant_topic_ids' in js_row:
            topics = js_row.pop('relevant_topic_ids')
        else:
            topics = None

        question_marks = ', '.join(
            ['?' for i in range(len(js_row.keys()))])
        insert_result = execute_insert_query(
            conn,
            ('INSERT INTO videos (\n' +
             ', '.join([f'\'{key}\'' for key in js_row.keys()]) +
             '\n)\nvalues (\n' + question_marks + '\n);'),
            values=[*js_row.values()])
        if not insert_result:
            sql_fails += 1
            success = False
        else:
            videos_inserted.append(js_row['id'])

        if tags:
            for tag in tags:
                if tag not in existing_tags:  # tag not in tags table
                    insert_result = execute_insert_query(
                        conn,
                        "INSERT INTO tags (tag) values (?)", tag)
                    if not insert_result:
                        sql_fails += 1
                        success = False
                    else:
                        tags_inserted.append(tag)
                cur = conn.cursor()
                try:
                    cur.execute(
                        f'SELECT id FROM tags WHERE tag = "{tag}"')
                    try:
                        tag_id = cur.fetchone()[0]

                        insert_result = execute_insert_query(
                            conn,
                            """INSERT INTO videos_tags (video_id, tag_id)
                            values (?, ?)""",
                            (js_row['id'], tag_id))
                    except TypeError:
                        err_inf = err_display(True)
                        logger.error(f'{err_inf}'
                                     f'\nvalues = {tag!r}')
                    if not insert_result:
                        sql_fails += 1
                        success = False
                except sqlite3.Error:
                    err_inf = err_display(True)
                    logger.error(f'{err_inf.err}\n'
                                 f'values = {tag!r}')
                finally:
                    cur.close()

        if topics:
            for topic in topics:
                if topic in children_topics:
                    insert_result = execute_insert_query(
                        conn,
                        """INSERT INTO videos_topics
                    (video_id, topic_id)
                    values (?, ?)""", (js_row['id'], topic))
                    if not insert_result:
                        sql_fails += 1
                        success = False
        if success:
            success_count += 1
            print('Successful attempts:', success_count)

    logger.info('-'*150 + f'\nPopulating finished'
                f'\nTotal fails: {sql_fails}')
    print('Total fails', sql_fails)


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
