import sqlite3
import logging

from typing import Union, Optional
from ktools import db
from ktools.dict_exploration import get_final_key_paths
from ktools.utils import timer
from config import video_keys_and_columns
from utils import convert_duration, logging_config

logging_config(r'C:\Users\Vladimir\Desktop\sql_fails.log')
logger = logging.getLogger(__name__)

MAIN_TABLE_COLUMNS = [column.split()[0] for column in
                      [
    'id text primary key',  # not using a separate integer as a PK
    'published_at timestamp',  # pass detect_types when creating connection
    'times_watched integer',
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
                   ]
                      ]
CHANNEL_COLUMNS = ['id', 'title']
TAG_COLUMNS = ['tag']  # id column (1st one) value is added implicitly by SQL
VIDEOS_TAGS_COLUMNS = ['video_id', 'tag_id']
VIDEOS_TOPICS_COLUMNS = ['video_id', 'topic_id']


def log_query_error(error, query_string: str, values):
    logger.error(f'Error: {error}\n'
                 f'query = \'{query_string}\'\n'
                 f'values = [{values}]')


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
            log_query_error(e, query, values)
        return False
    finally:
        cur.close()


def add_channel(conn: sqlite3.Connection,
                channel_id: str, channel_name: str = None) -> bool:
    values = [channel_id]
    if channel_name:
        values.append(channel_name)
    query_string = generate_insert_query('channels',
                                         columns=CHANNEL_COLUMNS[:len(values)])
    return execute_insert_query(conn, query_string, values)


def add_tag(conn: sqlite3.Connection, tag: str):
    query_string = generate_insert_query('tags', columns=TAG_COLUMNS)
    return execute_insert_query(conn, query_string, tag)


def add_video(conn: sqlite3.Connection, cols_vals: dict):
    query_string = generate_insert_query('videos', cols_vals)
    values = cols_vals.values()
    return execute_insert_query(conn, query_string, tuple(values))


def add_tag_to_video(conn: sqlite3.Connection, tag_id, video_id):
    values = video_id, tag_id
    query_string = generate_insert_query('videos_tags',
                                         columns=VIDEOS_TAGS_COLUMNS)
    try:
        return execute_insert_query(conn, query_string, values)
    except sqlite3.Error as e:
        log_query_error(e, query_string, values)
        return False


def add_topic(conn: sqlite3.Connection, topic, video_id):
    query_string = generate_insert_query('videos_topics',
                                         columns=VIDEOS_TOPICS_COLUMNS)
    values = video_id, topic
    return execute_insert_query(conn, query_string, values)


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
def populate_videos(db_path='db.sqlite'):
    import data_prep
    from topics import children_topics
    records = [wrangle_video_record_for_sql(record) for record in
               data_prep.get_videos_info_from_db(
                   r'G:\pyton\db.latestbackup', 'youtube_videos_info')]
    rows_passed = 0
    sql_fails = 0
    decl_types = sqlite3.PARSE_DECLTYPES
    decl_colnames = sqlite3.PARSE_COLNAMES
    conn = db.sqlite_connection(db_path,
                                detect_types=decl_types | decl_colnames)
    cur = conn.cursor()
    cur.execute("""SELECT id FROM videos;""")
    video_ids = [row[0] for row in cur.fetchall()]
    cur.execute("""SELECT id FROM channels;""")
    channels = [row[0] for row in cur.fetchall()]
    cur.execute("""SELECT * FROM tags;""")
    existing_tags = {v: k for k, v in cur.fetchall()}
    cur.close()
    logger.info(f'\nStarting records\' insertion...\n' + '-'*100)

    for video_record in records:
        rows_passed += 1
        # everything that doesn't go into the videos table gets popped
        # so the video table insert query can be constructed
        # automatically with the remaining items, since their amount
        # will vary from row to row
        if video_record['id'] in video_ids:
            continue

        if 'channel_title' in video_record:
            channel_title = video_record.pop('channel_title')
        else:
            channel_title = None
        channel_id = video_record['channel_id']

        if channel_id not in channels:
            if add_channel(conn, channel_id, channel_title):
                channels.append(video_record['channel_id'])
            else:
                sql_fails += 1

        if 'relevant_topic_ids' in video_record:
            topics = video_record.pop('relevant_topic_ids')
        else:
            topics = None
        if 'tags' in video_record:
            tags = video_record.pop('tags')
        else:
            tags = None

        # presence of this video gets checked at the very start of the loop
        if not add_video(conn, video_record):
            sql_fails += 1
        else:
            video_ids.append(video_record['id'])
        if tags:
            for tag in tags:
                if tag not in existing_tags:  # tag not in tags table
                    tag_id = None
                    if add_tag(conn, tag):
                        query_str = f'SELECT id FROM tags WHERE tag = "{tag}"'
                        cur = conn.cursor()
                        try:
                            cur.execute(query_str)
                            tag_id = cur.fetchone()[0]
                            existing_tags[tag] = tag_id
                            cur.close()
                        except (TypeError, sqlite3.Error) as e:
                            log_query_error(e, query_str, tag)
                    else:
                        sql_fails += 1
                else:
                    tag_id = existing_tags[tag]
                if tag_id:
                    if not add_tag_to_video(conn, tag_id, video_record['id']):
                        sql_fails += 1

        if topics:
            for topic in topics:
                if topic in children_topics:
                    if not add_topic(conn, topic, video_record['id']):
                        sql_fails += 1
        print(rows_passed)
    logger.info('-'*100 + f'\nPopulating finished'
                f'\nTotal fails: {sql_fails}')


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
    import cProfile
    cProfile.run("populate_videos('G:\\pyton\\db.latestbackup')")
