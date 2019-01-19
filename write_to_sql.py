import sqlite3
import logging
import youtube
from typing import Union
from utils import get_final_key_paths, convert_duration, sqlite_connection
from config import video_keys_and_columns

logger = logging.getLogger(__name__)
DB_NAME = 'yt.sqlite'
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
    on update cascade on delete cascade
    );''',

    'tags': '''tags (
    id integer primary key,
    tag text
    );''',

    'videos': '''videos (
    id text primary key,
    published_at timestamp,
    channel_id text,
    title text,
    description text,
    category_id text,
    default_audio_language text,
    duration integer,
    last_updated timestamp,
    status text,
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
    on update cascade on delete cascade
    );''',
    
    'videos_watched_at_timestamps': '''videos_watched_at_timestamps (
    video_id text,
    watched_at timestamp,
    unique(video_id, watched_at),
    foreign key (video_id) references videos (id)
    on update cascade on delete cascade
    );''',

    'failed_requests_ids': '''failed_requests_ids (
    id text primary key,
    attempts integer
    );''',

    # videos that have IDs in takeout, but aren't available via API and have no
    # minimal identifying data, such as title
    'dead_videos_ids': '''dead_videos_ids (
    id text primary key
    );'''
}

VIDEOS_COLUMNS = [column.split()[0] for column in
                  [
    'id text primary key',  # not using a separate integer as a PK
    'published_at timestamp',  # pass detect_types when creating connection
    'channel_id text',
    'title text',
    'description text',
    'category_id text',
    'default_audio_language text',
    'last_updated timestamp',
    'status text',
    'duration integer',
    'view_count integer',
    'like_count integer',
    'dislike_count integer',
    'comment_count integer',
                   ]
                  ]
CHANNEL_COLUMNS = ['id', 'title']
CATEGORIES_COLUMNS = ['id', 'channel_id', 'title', 'assignable', 'etag']
PARENT_TOPICS_COLUMNS = ['id', 'topic']
TOPICS_COLUMNS = ['id', 'topic', 'parent_topic_id']
TAGS_COLUMNS = ['tag']  # id column value is added implicitly by SQLite
VIDEOS_TAGS_COLUMNS = ['video_id', 'tag_id']
VIDEOS_TOPICS_COLUMNS = ['video_id', 'topic_id']
VIDEOS_WATCHED_AT_TIMESTAMPS_COLUMNS = ['video_id', 'watched_at']
FAILED_REQUESTS_IDS_COLUMNS = ['id', 'attempts']
DEAD_VIDEOS_COLUMNS = ['id']


def generate_insert_query(table: str,
                          columns: Union[list, tuple],
                          on_conflict: str = None)-> str:
    """
    Constructs a basic insert query.
    """

    val_amount = len(columns)
    values_placeholders = '(' + ('?, ' * val_amount).strip(' ,') + ')'
    columns = '(' + ', '.join(columns) + ')'

    query = f'INSERT INTO {table} {columns} VALUES {values_placeholders}'
    if on_conflict:
        query += f' ON CONFLICT {on_conflict}'
    return query


def generate_unconditional_update_query(table: str,
                                        columns: Union[list, tuple]):
    columns = ' = ?, '.join(columns).strip(',') + ' = ?'
    return f'''UPDATE {table} SET {columns} '''


# below are rigid insert queries, ones whose amount of columns will not change
# between records
# add_channel and add_video are compiled every run due to dynamic col amount
add_tag_query = generate_insert_query('tags', columns=TAGS_COLUMNS)
add_tag_to_video_query = generate_insert_query('videos_tags',
                                               columns=VIDEOS_TAGS_COLUMNS,
                                               on_conflict='IGNORE')
add_topic_to_video_query = generate_insert_query('videos_topics',
                                                 columns=VIDEOS_TOPICS_COLUMNS)
add_time_to_video_query = generate_insert_query(
    'videos_watched_at_timestamps',
    columns=VIDEOS_WATCHED_AT_TIMESTAMPS_COLUMNS, on_conflict='IGNORE')
add_failed_request_query = generate_insert_query(
    'failed_requests_ids',
    columns=FAILED_REQUESTS_IDS_COLUMNS, on_conflict='IGNORE')
add_dead_video_query = generate_insert_query('dead_videos_ids',
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
                  query: str, values: Union[list, tuple] = None,
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
            elif isinstance(values, tuple):
                pass
            else:
                raise ValueError('Expected str, tuple or list, got ' +
                                 values.__class__.__name__)
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
            logger.error('FATAL ERROR:')
            log_query_error(e, query, values)
        else:
            log_query_error(e, query)
        raise
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


def drop_dynamic_tables(conn):
    for table in TABLE_SCHEMAS:
        if table not in ['categories', 'topics', 'parent_topics',
                         'dead_videos_ids']:
            execute_query(conn, '''DROP TABLE IF EXISTS ''' + table)
    conn.commit()
    conn.close()


def add_channel(conn: sqlite3.Connection,
                channel_id: str, channel_name: str = None) -> bool:
    values = [channel_id]
    if channel_name:
        values.append(channel_name)
    query_string = generate_insert_query('channels',
                                         columns=CHANNEL_COLUMNS[:len(values)])
    return execute_query(conn, query_string, values)


def add_tag(conn: sqlite3.Connection, tag: str):
    return execute_query(conn, add_tag_query, (tag,))


def add_video(conn: sqlite3.Connection, cols_vals: dict):
    query_string = generate_insert_query('videos', list(cols_vals.keys()))
    values = cols_vals.values()
    return execute_query(conn, query_string, tuple(values))


def update_video(conn: sqlite3.Connection, cols_vals: dict):
    """Updates all fields, even if the values are the same; seems cheaper than
    retrieving the full record and then checking which fields are different."""
    query_string = generate_unconditional_update_query(
        'videos', list(cols_vals.keys()))
    values = cols_vals.values()
    return execute_query(conn, query_string, tuple(values))


def add_tag_to_video(conn: sqlite3.Connection, tag_id, video_id):
    values = video_id, tag_id
    return execute_query(conn, add_tag_to_video_query, values)


def add_topic_to_video(conn: sqlite3.Connection, topic, video_id):
    values = video_id, topic
    return execute_query(conn, add_topic_to_video_query, values)


def add_time(conn: sqlite3.Connection, watched_at, video_id):
    values = video_id, watched_at
    return execute_query(conn, add_time_to_video_query, values)


def add_failed_request(conn: sqlite3.Connection, video_id, attempts: int):
    if attempts == 1:
        execute_query(conn,
                      add_failed_request_query,
                      (video_id, 1), log_integrity_fail=False)
    else:
        execute_query(conn,
                      '''UPDATE failed_requests_ids 
                      SET attempts = ?
                      WHERE id = ?''',
                      (attempts, video_id))
        return True


def add_dead_video(conn: sqlite3.Connection, video_id):
    return execute_query(conn, add_dead_video_query, (video_id,))


def insert_or_refresh_categories():
    def bool_adapt(bool_value: bool): return str(bool_value)

    sqlite3.register_adapter(bool, bool_adapt)
    categories = youtube.get_categories()
    query_string = generate_insert_query('categories',
                                         columns=CATEGORIES_COLUMNS)
    conn = sqlite_connection(DB_NAME, detect_types=sqlite3.PARSE_DECLTYPES)
    if execute_query(conn, 'DELETE FROM categories;'):
        for category_dict in categories['items']:
            etag = category_dict['etag']
            id_ = category_dict['id']
            channel_id = category_dict['snippet']['channelId']
            title = category_dict['snippet']['title']
            assignable = category_dict['snippet']['assignable']
            execute_query(conn, query_string,
                          (id_, channel_id, title, assignable, etag),
                          log_integrity_fail=False)
        conn.commit()
    conn.close()


def insert_parent_topics():
    from topics import topics_by_category

    conn = sqlite_connection(DB_NAME)
    query_string = generate_insert_query('parent_topics',
                                         columns=PARENT_TOPICS_COLUMNS)

    for topic_dict in topics_by_category.values():
        for k, v in topic_dict.items():
            parent_topic_str = ' (parent topic)'
            if parent_topic_str in v:
                v = v.replace(parent_topic_str, '')
                execute_query(conn, query_string, (k, v),
                              log_integrity_fail=False)
            break

    conn.commit()
    conn.close()


def insert_sub_topics():
    from topics import topics_by_category

    conn = sqlite_connection(DB_NAME)
    query_string = generate_insert_query('topics',
                                         columns=TOPICS_COLUMNS)

    for topic_dict in topics_by_category.values():
        parent_topic_str = ' (parent topic)'
        parent_topic_name = None
        for k, v in topic_dict.items():
            if parent_topic_str in v:
                parent_topic_name = k
                continue
            insert_tuple = (k, v, parent_topic_name)
            execute_query(conn, query_string, insert_tuple,
                          log_integrity_fail=False)

    conn.commit()
    conn.close()


def setup_tables():
    conn = sqlite_connection(DB_NAME)
    cur = conn.cursor()
    for schema in TABLE_SCHEMAS:
        schema_ = 'CREATE TABLE IF NOT EXISTS ' + TABLE_SCHEMAS[schema]
        try:
            cur.execute(schema_)
            conn.commit()
        except sqlite3.Error as e:
            print(e)
            print(schema_)

    add_channel(conn, 'unknown', 'unknown')
    add_video(conn,
              {'title': 'unknown', 'channel_id': 'unknown', 'id': 'unknown'})
    conn.commit()
    conn.close()
    insert_or_refresh_categories()
    insert_parent_topics()
    insert_sub_topics()


def insert_videos():

    from convert_takeout import get_all_records
    import time
    from datetime import datetime
    from config import DEVELOPER_KEY
    rows_passed = 0
    sql_fails = 0
    api_requests_fails = 0
    decl_types = sqlite3.PARSE_DECLTYPES
    decl_colnames = sqlite3.PARSE_COLNAMES
    conn = sqlite_connection(DB_NAME, detect_types=decl_types | decl_colnames)
    cur = conn.cursor()
    cur.execute("""SELECT id FROM videos;""")
    video_ids = [row[0] for row in cur.fetchall()]
    cur.execute("""SELECT id FROM channels;""")
    channels = [row[0] for row in cur.fetchall()]
    cur.execute("""SELECT * FROM tags;""")
    existing_tags = {v: k for k, v in cur.fetchall()}
    cur.execute("""SELECT * FROM videos_watched_at_timestamps;""")
    all_timestamps = {}
    for timestamp_record in cur.fetchall():
        all_timestamps.setdefault(timestamp_record[0], [])
        all_timestamps[timestamp_record[0]].append(timestamp_record[1])
    cur.execute("""SELECT id FROM dead_videos_ids;""")
    dead_videos_ids = [dead_video[0] for dead_video in cur.fetchall()]
    cur.execute("""SELECT * FROM failed_requests_ids;""")
    failed_requests_ids = {k: v for k, v in cur.fetchall()}
    cur.close()
    logger.info(f'\nStarting records\' insertion...\n' + '-'*100)
    records = get_all_records('takeout_data')

    yt_api = youtube.get_api_auth(DEVELOPER_KEY)
    for video_record in records:
        video_id = video_record
        rows_passed += 1
        video_record = records[video_record]
        video_record['id'] = video_id

        if video_id in video_ids:
            timestamps = video_record.pop('timestamps')
            for timestamp in timestamps:
                if timestamp not in all_timestamps[video_id]:
                    add_time(conn, timestamp, video_id)

            if video_id in dead_videos_ids and 'title' in video_record:
                # Older Takeout file which for some reason was added out of
                # order and which has info on a video that has been
                # deleted by the time newer Takeout containing entries for that
                # video has been generated
                if 'channel_id' in video_record:
                    channel_id = video_record['channel_id']
                    add_channel(conn, channel_id)
                update_video(conn, video_record)
            conn.commit()
            continue

        elif video_id in failed_requests_ids:
            pass
        else:
            '''
            Things that can change:
                video title
                channel title
                category
                all the counts (view, like, etc.)
                
            Add two columns: last_updated, video_status (available or not)
            
            Tasks to implement functions for:
              Add new records and update timestamps for the existing 
                ones. This uses Takeout data. Uses API requests for new 
                records.
              Update existing records, i.e. their view counts, possibly 
                names, anything that may have changed. Uses API requests.
            
            This function can be modified to do the first task.
            The second task must have a separate function.
            '''
            for timestamp in video_record['timestamps']:
                if timestamp not in all_timestamps:
                    add_time(conn, timestamp, video_id)
            continue

        print(rows_passed, 'entries processed')
        for attempt in range(1, 6):
            api_response = youtube.get_video_info(video_id, yt_api)
            time.sleep(0.01*attempt**attempt)
            if api_response:
                if api_response['items']:
                    api_response = wrangle_video_record(
                        api_response['items'])
                    video_record.update(api_response)
                else:
                    add_dead_video(conn, video_id)
                break
        else:
            failed_requests_ids.setdefault(video_id, 0)
            attempts = failed_requests_ids[video_id]
            if attempts > 1:
                add_dead_video(conn, video_id)
                execute_query(conn,
                              '''DELETE FROM failed_requests_id
                              WHERE id = ?''', (video_id,))
            else:
                attempts += 1
                add_failed_request(conn, video_id,
                                   attempts)
            logger.error(
                f'Failed API request, ID# {video_id}')
            # video is still inserted, as long as the minimum data necessary
            # is present (title), but this will be run again a couple of times;
            # it will attempt to retrieve the data from API and
            # update the record accordingly, if any data is returned

            for field in ['channel_id', 'title']:
                if field not in video_record:
                    video_record[field] = 'unknown'

        # everything that doesn't go into the videos table gets popped below
        # so the videos
        # table insert query can be constructed
        # automatically with the remaining items, since their amount
        # will vary from row to row

        if 'channel_title' in video_record:
            channel_title = video_record.pop('channel_title')
        else:
            channel_title = None
        channel_id = video_record['channel_id']

        if channel_id not in channels:
            if add_channel(conn, channel_id, channel_title):
                channels.append(video_record['channel_id'])
            else:
                continue  # nothing else can/should be inserted without the
                # channel for it being inserted

        if 'relevant_topic_ids' in video_record:
            topics = video_record.pop('relevant_topic_ids')
        else:
            topics = None
        if 'tags' in video_record:
            tags = video_record.pop('tags')
        else:
            tags = None

        timestamps = video_record.pop('timestamps')

        if video_id in video_ids:  # passing this check means the API request
            # has been successfully made, whereas previous attempts have failed.
            # Had they not failed, this check would have
            # never been reached.
            update_video(conn, video_record)
        else:
            if add_video(conn, video_record):
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
                            # this is thrown because sometimes the tag_id is not
                            # retrieved, due to a weird combo of escaping and
                            # the tag being an SQLITE keyword.
                            # It's likely fixed now (after changing the select
                            # query a bit) and won't error out anymore, but
                            # needs to be confirmed first
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
                    tag_id = existing_tags[tag]
                if tag_id:
                    add_tag_to_video(conn, tag_id, video_id)

        if topics:
            for topic in topics:
                add_topic_to_video(conn, topic, video_id)

        conn.commit()  # committing after every record ensures all its info
        # is inserted, or not at all, in case of an unforeseen failure during
        # some loop

    conn.commit()
    conn.close()
    logger.info('-'*100 + f'\nPopulating finished'
                f'\nTotal SQL fails: {sql_fails}'
                f'\nTotal API fails: {api_requests_fails}')


def setup_db():
    setup_tables()
    insert_or_refresh_categories()
    insert_parent_topics()
    insert_sub_topics()


def test_it():
    conn = sqlite_connection(':memory:')
    cur = conn.cursor()
    cur.execute('create table a (b text, c text);')
    cur.execute('select * from a')
    print(cur.fetchall())


if __name__ == '__main__':
    # setup_tables()
    print(generate_unconditional_update_query('boi', ['col1', 'col2']))
