import os
import json
import sqlite3
import logging
from typing import Union, Optional
from utils import get_final_key_paths, logging_config
from utils import convert_duration, sqlite_connection
from ktools.utils import timer
from config import video_keys_and_columns
from convert_takeout import get_all_records

logging_config(r'C:\Users\Vladimir\Desktop\sql_fails.log')
logger = logging.getLogger(__name__)

TABLE_SCHEMAS = {
    'categories': '''categories (
    id text primary key,
    channel_id text,
    title text,
    assignable bool,
    etag text
    );''',

    'channels': '''channels (
    id text primary key,
    title text
    );''',

    'parent_topics': '''parent_topics (
    id text primary key,
    topic text
    );''',

    'topics': '''topics (
    id text primary key,
    topic text,
    parent_topic_id text,
    foreign key (parent_topic_id) references parent_topics(id)
    on delete cascade on update cascade
    );''',

    'tags': '''tags (
    id integer primary key,
    tag text
    );''',

    'videos': '''videos (
    id text primary key,
    published_at timestamp,
    times_watched integer,
    channel_id text,
    title text,
    description text,
    category_id text,
    default_audio_language text,
    duration integer,
    view_count integer,
    like_count integer,
    dislike_count integer,
    comment_count integer,
    foreign key (channel_id) references channels (id)
    on update cascade on delete cascade
    );''',

    'videos_tags': '''videos_tags (
    video_id text,
    tag_id text,
    unique (video_id, tag_id),
    foreign key (video_id) references videos (id)
    on update cascade on delete cascade,
    foreign key (tag_id) references tags (id)
    on update cascade on delete cascade
    );''',

    'videos_topics': '''videos_topics (
    video_id text,
    topic_id text,
    foreign key (video_id) references videos (id)
    on update cascade on delete cascade,
    foreign key (topic_id) references topics (id)
    on update cascade on delete cascade
    );''',

    'videos_watched_at_timestamps': '''videos_watched_at_timestamps (
    video_id text,
    watched_at timestamp,
    foreign key (video_id) references videos (id)
    on update cascade on delete cascade
    );''',

    'failed_requests_ids': '''failed_requests_ids (
    id text primary key,
    attempts integer
    );''',

    'dead_videos': '''dead_videos (
    id text primary key
    );'''
}

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
TAGS_COLUMNS = ['tag']  # id column value is added implicitly by SQLite
VIDEOS_TAGS_COLUMNS = ['video_id', 'tag_id']
VIDEOS_TOPICS_COLUMNS = ['video_id', 'topic_id']
VIDEOS_WATCHED_AT_TIMESTAMPS_COLUMNS = ['video_id', 'watched_at']
FAILED_REQUESTS_IDS_COLUMNS = ['id', 'attempts']
DEAD_VIDEOS_COLUMNS = ['id']


def generate_insert_query(table: str,
                          columns_values: Optional[dict] = None,
                          columns: Optional[Union[list, tuple]] = None)-> str:
    """
    Constructs a basic insert query.
    Requires only one of the optional kwargs to be filled.
    """
    if not columns_values and not columns:
        raise ValueError('Nothing was passed.')

    if columns_values:
        columns = columns_values.keys()

    val_amount = len(columns)
    values_placeholders_str = '(' + ('?, ' * val_amount).strip(' ,') + ')'
    columns_str = '(' + ', '.join(columns) + ')'

    return f'INSERT INTO {table} {columns_str} VALUES {values_placeholders_str}'


# below are rigid insert queries, ones whose columns will not change
# between records
# add_channel and add_video are not rigid and have to be compiled every run
add_tag_query = generate_insert_query('tags', columns=TAGS_COLUMNS)
add_tag_to_video_query = generate_insert_query('videos_tags',
                                               columns=VIDEOS_TAGS_COLUMNS)
add_topic_query = generate_insert_query('videos_topics',
                                        columns=VIDEOS_TOPICS_COLUMNS)
add_time_query = generate_insert_query(
    'videos_watched_at_timestamps',
    columns=VIDEOS_WATCHED_AT_TIMESTAMPS_COLUMNS)
add_failed_request_query = generate_insert_query(
    'failed_requests_ids',
    columns=FAILED_REQUESTS_IDS_COLUMNS)
add_dead_video_query = generate_insert_query('dead_videos',
                                             columns=DEAD_VIDEOS_COLUMNS)


def log_query_error(error, query_string: str, values=None):
    if not values:
        logger.error(f'{error}\n'
                     f'query = \'{query_string}\'')
        return
    logger.error(f'{error}\n'
                 f'query = \'{query_string}\'\n'
                 f'values = {values}')


def execute_query(conn: sqlite3.Connection,
                  query: str, values: Union[list, str, tuple] = None,
                  log_integrity_fail=True):
    """
    Executes the query with passed values (if any). If a SELECT query,
    returns the results.
    Passes potential errors to a logger. Logging for integrity errors,
    such as a foreign key constraint fail, can be disabled
    via log_integrity_fail param.
    """
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
        if query.lower().startswith('select'):
            return cur.fetchall()
        return True
    except sqlite3.IntegrityError as e:
        if not log_integrity_fail:
            return False
        if values:
            values = f'{list(values)}'
            log_query_error(e, query, values)
        else:
            log_query_error(e, query)
        return False
    except sqlite3.Error as e:
        if values:
            values = f'{list(values)}'
            log_query_error(e, query, values)
        else:
            log_query_error(e, query)
        return False
    finally:
        cur.close()


def wrangle_video_record(json_obj: dict):
    """
    Extracts the keys deemed necessary from a Youtube API response, converts
    some of them to the right types and returns them in a dict
    """
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


def create_all_tables(db_path: str):
    conn = sqlite_connection(db_path)
    cur = conn.cursor()
    for schema in TABLE_SCHEMAS:
        schema_ = 'CREATE TABLE IF NOT EXISTS ' + TABLE_SCHEMAS[schema]
        try:
            cur.execute(schema_)
            conn.commit()
        except sqlite3.Error as e:
            print(e)
            print(schema_)


def add_channel(conn: sqlite3.Connection,
                channel_id: str, channel_name: str = None) -> bool:
    values = [channel_id]
    if channel_name:
        values.append(channel_name)
    query_string = generate_insert_query('channels',
                                         columns=CHANNEL_COLUMNS[:len(values)])
    return execute_query(conn, query_string, values)


def add_tag(conn: sqlite3.Connection, tag: str):
    return execute_query(conn, add_tag_query, tag)


def add_video(conn: sqlite3.Connection, cols_vals: dict):
    query_string = generate_insert_query('videos', cols_vals)
    values = cols_vals.values()
    return execute_query(conn, query_string, tuple(values))


def add_tag_to_video(conn: sqlite3.Connection, tag_id, video_id):
    values = video_id, tag_id
    return execute_query(conn, add_tag_to_video_query, values,
                         log_integrity_fail=False)


def add_topic(conn: sqlite3.Connection, topic, video_id):
    values = video_id, topic
    return execute_query(conn, add_topic_query, values)


def add_time(conn: sqlite3.Connection, watched_at, video_id):
    values = video_id, watched_at
    return execute_query(conn, add_time_query, values)


def add_failed_request(conn: sqlite3.Connection, video_id):
    if not execute_query(conn,
                         add_failed_request_query,
                         (video_id, 1), log_integrity_fail=False):
        result = execute_query(conn,
                               '''SELECT attempts from failed_requests_ids 
                               WHERE id = ?''',
                               (video_id,))
        if result:
            current_attempt = result[0][0] + 1
            execute_query(conn,
                          '''UPDATE failed_requests_ids 
                          SET attempts = ?
                          WHERE id = ?''',
                          (current_attempt, video_id))
            return True


def add_dead_video(conn: sqlite3.Connection, video_id):
    return execute_query(conn, add_dead_video_query, (video_id,))


@timer
def insert_videos(db_path: str, takeout_path: str = None):

    import time
    import youtube
    from datetime import datetime
    from topics import children_topics
    from confidential import DEVELOPER_KEY
    if not DEVELOPER_KEY:  # this and the line above must be removed once this
        # is a library
        if os.path.exists('api_key'):
            with open('api_key', 'r') as file:
                DEVELOPER_KEY = file.read().strip()
        else:
            raise SystemExit(
                'An API key must be assigned to DEVELOPER_KEY '
                'to retrieve video info.')
    rows_passed = 0
    sql_fails = 0
    api_requests_fails = 0
    decl_types = sqlite3.PARSE_DECLTYPES
    decl_colnames = sqlite3.PARSE_COLNAMES
    conn = sqlite_connection(db_path, detect_types=decl_types | decl_colnames)
    cur = conn.cursor()
    cur.execute("""SELECT id FROM videos;""")
    video_ids = [row[0] for row in cur.fetchall()]
    cur.execute("""SELECT id FROM channels;""")
    channels = [row[0] for row in cur.fetchall()]
    cur.execute("""SELECT * FROM tags;""")
    existing_tags = {v: k for k, v in cur.fetchall()}
    cur.execute("""SELECT id FROM dead_videos;""")
    dead_videos = [dead_video[0] for dead_video in cur.fetchall()]
    cur.close()
    logger.info(f'\nStarting records\' insertion...\n' + '-'*100)
    records = get_all_records(takeout_path)
    unknown = ''
    if 'unknown' in records:
        # gets popped now, but added at the end because other videos which have
        # IDs will also turn out to be unavailable and their timestamps will be
        # added to the unknown record so it could be inserted just once instead
        # of inserting and then updating it every time an unavailable video is
        # encountered.
        unknown: dict = records.pop('unknown')
        unknown.update({'id': 'unknown', 'channel_id': 'unknown'})

    # when looping through records, if an empty one is returned, it's to be
    # checked for any extra info present in its Takeout record and if none
    # exists, have it added as unknown, along with updating times_watched and
    # adding the timestamps of those times
    yt_api = youtube.get_api_auth(DEVELOPER_KEY)
    for video_record in records:
        video_id = video_record
        rows_passed += 1
        video_record = records[video_record]
        video_record['id'] = video_id
        if video_id in dead_videos:  # necessary because unknown record
            # only gets added at the very end, after having been updated with
            # all the empty ones returned from API requests
            unknown['times_watched'] += video_record['times_watched']
            unknown['timestamps'].extend(video_record['timestamps'])
            continue
        if video_id in video_ids:
            continue
        # everything that doesn't go into the videos table gets popped
        # so the video table insert query can be constructed
        # automatically with the remaining items, since their amount
        # will vary from row to row

        # if rows_passed % 200 == 0:
        print(rows_passed, 'entries processed')
        for attempt in range(1, 6):
            api_response = youtube.get_video_info(video_id, yt_api)
            time.sleep(0.01*attempt**attempt)
            if api_response:
                if api_response['items']:
                    api_response = wrangle_video_record(api_response['items'])
                    video_record.update(api_response)
                break
        else:
            api_requests_fails += 1
            add_failed_request(conn, video_id)
            logger.error(
                f'Failed API request {rows_passed}, ID# {video_id}')

        if 'title' not in video_record:
            unknown['times_watched'] += video_record['times_watched']
            unknown['timestamps'].extend(video_record['timestamps'])
            add_dead_video(conn, video_id)
            continue
        else:
            if 'channel_id' not in video_record:
                # a few videos have a title, but not channel info
                video_record['channel_id'] = 'unknown'

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
            
        timestamps = video_record.pop('timestamps')

        # presence of this video gets checked at the very start of the loop
        if not add_video(conn, video_record):
            sql_fails += 1
        else:
            video_ids.append(video_id)
            
        for timestamp in timestamps:
            timestamp = datetime.strptime(timestamp[:-4],
                                          '%b %d, %Y, %I:%M:%S %p')
            add_time(conn, timestamp, video_id)

        if tags:
            for tag in tags:
                if tag not in existing_tags:
                    tag_id = None
                    if add_tag(conn, tag):
                        query_str = 'SELECT id FROM tags WHERE tag = ?'
                        cur = conn.cursor()
                        cur.execute(query_str, (tag,))
                        try:
                            tag_id = cur.fetchone()[0]
                            existing_tags[tag] = tag_id
                        except (TypeError, sqlite3.Error) as e:
                            max_id_query_str = 'SELECT max(id) FROM tags'
                            cur.execute(max_id_query_str)
                            tag_id = cur.fetchone()[0] + 1
                            existing_tags[tag] = tag_id
                            update_tag_id_query = '''UPDATE tags 
                            SET id = ? WHERE tag = ?'''
                            cur.execute(update_tag_id_query, (tag_id, tag))
                            log_query_error(e, query_str, tag)
                        cur.close()
                    else:
                        sql_fails += 1
                else:
                    tag_id = existing_tags[tag]
                if tag_id:
                    add_tag_to_video(conn, tag_id, video_id)

        if topics:
            for topic in topics:
                if topic in children_topics:
                    if not add_topic(conn, topic, video_id):
                        sql_fails += 1

        conn.commit()  # committing after every record ensures all its info
        # is inserted, or not at all, in case of an unforeseen failure during
        # some loop

    if unknown:
        # unknown is recalculated and populated anew every time as there's no
        # way of reliably tracking individual records
        execute_query(conn,
                      '''DELETE FROM videos where id = ?''',
                      ('unknown',))
        execute_query(conn,
                      '''DELETE FROM videos_watched_at_timestamps
                      WHERE video_id = ?''',
                      ('unknown',))
        timestamps = unknown.pop('timestamps')
        add_channel(conn, channel_id='unknown')
        add_video(conn, cols_vals=unknown)
        for timestamp in timestamps:
            timestamp = datetime.strptime(
                timestamp[:-4], '%b %d, %Y, %I:%M:%S %p')
            add_time(conn, timestamp, 'unknown')

    conn.commit()
    conn.close()
    logger.info('-'*100 + f'\nPopulating finished'
                f'\nTotal SQL fails: {sql_fails}'
                f'\nTotal API fails: {api_requests_fails}')


def insert_categories(db_path, project_path):
    def bool_adapt(bool_value: bool): return str(bool_value)

    def bool_retrieve(bool_str): return eval(bool_str)

    sqlite3.register_adapter(bool, bool_adapt)
    sqlite3.register_converter('bool', bool_retrieve)
    conn = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES)
    with open(os.path.join(project_path, 'categories.json'), 'r') as file:
        cat_json = json.load(file)
    for category_dict in cat_json['items']:
        etag = category_dict['etag']
        id_ = category_dict['id']
        channel_id = category_dict['snippet']['channelId']
        title = category_dict['snippet']['title']
        assignable = category_dict['snippet']['assignable']

        cur = conn.cursor()
        cur.execute("""insert into categories 
        (id, channel_id, title, assignable, etag) 
        values (?, ?, ?, ?, ?)""", (id_, channel_id, title, assignable, etag))
        conn.commit()
        cur.close()

    conn.close()

def retrieve_test():
    conn = sqlite3.connect(':memory:')
    cur = conn.cursor()
    cur.execute('CREATE TABLE ' + TABLE_SCHEMAS['failed_requests_ids'])
    cur.execute('INSERT INTO failed_requests_ids values (?, ?)',
                ('some_id', 1))
    add_failed_request(conn, 'some_id')
    cur.execute('select * from failed_requests_ids')
    print(cur.fetchall())


if __name__ == '__main__':
    def drop_dynamic_tables(conn):
        for table in TABLE_SCHEMAS:
            if table not in ['categories', 'topics', 'parent_topics',
                             'dead_videos']:
                execute_query(conn, '''DROP TABLE IF EXISTS ''' + table)
        conn.commit()
        conn.close()
    # import legacy_sql
    # legacy_sql.populate_categories_into_sql(test_db_path)
    # legacy_sql.populate_parent_topics_into_sql(test_db_path)
    # legacy_sql.populate_sub_topics_into_sql(test_db_path)
    test_db_path = r'G:\pyton\first_test.sqlite'
    # drop_dynamic_tables(sqlite3.Connection(test_db_path))
    # create_all_tables(test_db_path)
    insert_videos(test_db_path,
                           r'G:\pyton\youtube_watched_data\takeout_data')
