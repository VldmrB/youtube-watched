import sqlite3
import json
import logging
import youtube
from typing import Union
from utils import get_final_key_paths, convert_duration, sqlite_connection
from config import video_keys_and_columns

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

CHANNEL_COLUMNS = ['id', 'title']
CATEGORIES_COLUMNS = ['id', 'channel_id', 'title', 'assignable', 'etag']
PARENT_TOPICS_COLUMNS = ['id', 'topic']
TOPICS_COLUMNS = ['id', 'topic', 'parent_topic_id']
TAGS_COLUMNS = ['tag']  # id column value is added implicitly by SQLite
VIDEOS_TAGS_COLUMNS = ['video_id', 'tag_id']
VIDEOS_TOPICS_COLUMNS = ['video_id', 'topic_id']
VIDEOS_WATCHED_AT_TIMESTAMPS_COLUMNS = ['video_id', 'watched_at']
FAILED_REQUESTS_IDS_COLUMNS = ['id', 'attempts']
DEAD_VIDEOS_IDS_COLUMNS = ['id']


def generate_insert_query(table: str,
                          columns: Union[list, tuple],
                          on_conflict_ignore=False)-> str:
    """
    Constructs a basic insert query.
    """

    val_amount = len(columns)
    values_placeholders = '(' + ('?, ' * val_amount).strip(' ,') + ')'
    columns = '(' + ', '.join(columns) + ')'

    query = f' INTO {table} {columns} VALUES {values_placeholders}'
    if on_conflict_ignore:
        query = f'INSERT OR IGNORE' + query
    else:
        query = 'INSERT' + query
    return query


def generate_unconditional_update_query(table: str,
                                        columns: Union[list, tuple]):
    columns = ' = ?, '.join(columns).strip(',') + ' = ?'
    return f'''UPDATE {table} SET {columns} WHERE id = ?'''


# below are rigid insert queries, ones whose amount of columns will not change
# between records
# add_channel and add_video are compiled every run due to dynamic col amount
add_tag_query = generate_insert_query('tags', columns=TAGS_COLUMNS)
add_tag_to_video_query = generate_insert_query('videos_tags',
                                               columns=VIDEOS_TAGS_COLUMNS,
                                               on_conflict_ignore=True)
add_topic_to_video_query = generate_insert_query('videos_topics',
                                                 columns=VIDEOS_TOPICS_COLUMNS)
add_time_to_video_query = generate_insert_query(
    'videos_watched_at_timestamps',
    columns=VIDEOS_WATCHED_AT_TIMESTAMPS_COLUMNS, on_conflict_ignore=True)
add_failed_request_query = generate_insert_query(
    'failed_requests_ids',
    columns=FAILED_REQUESTS_IDS_COLUMNS)
add_dead_video_query = generate_insert_query('dead_videos_ids',
                                             columns=DEAD_VIDEOS_IDS_COLUMNS,
                                             on_conflict_ignore=True)


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
                                         CHANNEL_COLUMNS[:len(values)],
                                         True)
    return execute_query(conn, query_string, values)


def add_tag(conn: sqlite3.Connection, tag: str):
    return execute_query(conn, add_tag_query, (tag,))


def add_video(conn: sqlite3.Connection, cols_vals: dict):
    query_string = generate_insert_query('videos', list(cols_vals.keys()), True)
    values = cols_vals.values()
    return execute_query(conn, query_string, tuple(values))


def update_video(conn: sqlite3.Connection, cols_vals: dict):
    """Updates all fields, even if the values are the same; seems cheaper than
    retrieving the full record and then checking which fields are different."""
    video_id = cols_vals.pop('id')
    query_string = generate_unconditional_update_query(
        'videos', list(cols_vals.keys()))
    values = list(cols_vals.values())
    values.append(video_id)
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


def delete_failed_request(conn: sqlite3.Connection, video_id):
    return execute_query(conn,
                         '''DELETE FROM failed_requests_ids
                         WHERE id = ?''', (video_id,))


def add_dead_video(conn: sqlite3.Connection, video_id):
    return execute_query(conn, add_dead_video_query, (video_id,))


def delete_dead_video(conn: sqlite3.Connection, video_id):
    return execute_query(conn,
                         '''DELETE FROM dead_videos_ids
                         WHERE id = ?''', (video_id,))


def insert_or_refresh_categories(db_path: str, api_auth, refresh=True):
    def bool_adapt(bool_value: bool): return str(bool_value)

    sqlite3.register_adapter(bool, bool_adapt)
    categories = youtube.get_categories(api_auth)
    query_string = generate_insert_query('categories',
                                         columns=CATEGORIES_COLUMNS)
    conn = sqlite_connection(db_path, detect_types=sqlite3.PARSE_DECLTYPES)
    if refresh or not execute_query(conn, 'SELECT * FROM categories'):
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


def insert_parent_topics(db_path: str):
    from topics import topics_by_category

    conn = sqlite_connection(db_path)
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


def insert_sub_topics(db_path: str):
    from topics import topics_by_category

    conn = sqlite_connection(db_path)
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


def setup_tables(db_path: str, api_auth):
    conn = sqlite_connection(db_path)
    cur = conn.cursor()
    for schema in TABLE_SCHEMAS:
        schema_ = 'CREATE TABLE IF NOT EXISTS ' + TABLE_SCHEMAS[schema]
        try:
            cur.execute(schema_)
        except sqlite3.Error as e:
            print(e)
            print(schema_)
    insert_or_refresh_categories(db_path, api_auth, False)
    insert_parent_topics(db_path)
    insert_sub_topics(db_path)

    conn.commit()
    conn.close()


def insert_videos(db_path: str, records: dict, api_auth,
                  generate_progress=False):

    import time
    from datetime import datetime
    rows_passed, inserted, updated, failed, dead = 0, 0, 0, 0, 0
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

    '''
    Add two columns: 
        last_updated
        status (available or not, depends on API response)
    
    Tasks to implement functions for:
      1. Add new records and update timestamps for the existing ones. 
      This uses Takeout data. Uses API requests for new records.
      2. Update existing records, i.e. their view counts, possibly names, 
      anything that may have changed. Uses API requests. 
      Things that may/will change in a record over time: 
       - video
       - title
       - channel
       - title
       - category 
       - all the counts (view, like, etc.) 
    
    This function can be modified to do the first task.
    The second task must have a separate function.
    '''

    # due to its made up ID, the unknown record is best handled manually
    if 'unknown' in records:
        rows_passed += 1
        unknown_record = records.pop('unknown')
        unknown_record['id'] = 'unknown'
        unknown_record['timestamps'] = [
            datetime.strptime(timestamp[:-4], '%b %d, %Y, %I:%M:%S %p')
            for timestamp in unknown_record['timestamps']
        ]
        unknown_timestamps = unknown_record.pop('timestamps')
        unknown_record['status'] = 'inactive'
        add_channel(conn, 'unknown', 'unknown')
        add_video(conn, unknown_record)
        all_timestamps.setdefault('unknown', [])
        for timestamp in unknown_timestamps:
            if timestamp not in all_timestamps['unknown']:
                add_time(conn, timestamp, 'unknown')
    records_len = len(records)
    percent = records_len / 1000
    percent_int = int(percent)
    yt_api = api_auth
    for video_id, video_record in records.items():
        rows_passed += 1
        if generate_progress:
            if rows_passed % percent_int == 0:
                print(f'Processing entry # {rows_passed}')
                yield (rows_passed // percent)/10
        video_record['id'] = video_id
        video_record['timestamps'] = [
            datetime.strptime(timestamp[:-4], '%b %d, %Y, %I:%M:%S %p')
            for timestamp in video_record['timestamps']
        ]

        if video_id not in video_ids or video_id in failed_requests_ids:
            pass
        else:
            '''
            This block deals with Takeout records already in the table. There 
            should only be two reasons for triggering it: 
                1. Updating timestamps for the record, in case the video has 
                been watched again.
                2. If an older Takeout file was added (out of order, that is) 
                and contains info which was not available in the Takeouts 
                already processed, which resulted in a video being added with 
                an unknown title, for example. That should only happen if the 
                video in question was still available when the older Takeout was 
                generated, but was deleted by the time the newer Takeout was.
            '''
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
                    if 'channel_title' in video_record:
                        channel_title = video_record.pop('channel_title')
                    else:
                        channel_title = None
                    add_channel(conn, video_record['channel_id'], channel_title)
                video_record['status'] = 'inactive'
                video_record['last_updated'] = datetime.utcnow(
                ).replace(microsecond=0)
                if update_video(conn, video_record):
                    delete_dead_video(conn, video_id)
                    updated += 1
            conn.commit()
            continue

        for attempt in range(1, 6):
            api_response = youtube.get_video_info(video_id, yt_api)
            time.sleep(0.01*attempt**attempt)
            if api_response:
                delete_failed_request(conn, video_id)  # there may not be a
                # record for this yet due to one only being created after 5
                # failed attempts, while this checks regardless of the number
                # of those, but that's fine
                if api_response['items']:
                    api_response = wrangle_video_record(api_response['items'])
                    video_record.update(api_response)
                    video_record['status'] = 'active'
                else:
                    video_record['status'] = 'inactive'

                video_record['last_updated'] = datetime.utcnow(
                ).replace(microsecond=0)
                break
        else:
            failed_requests_ids.setdefault(video_id, 0)
            attempts = failed_requests_ids[video_id]
            if attempts + 1 > 2:
                video_record['status'] = 'inactive'
                video_record['last_updated'] = datetime.utcnow(
                ).replace(microsecond=0)
                delete_failed_request(conn, video_id)
            else:
                if add_failed_request(conn, video_id, attempts + 1):
                    failed += 1
            # video is still inserted, but this will be run again a couple of
            # times; it will attempt to retrieve the data from API and
            # update the record accordingly, if any data is returned

        if 'title' not in video_record:
            video_record['title'] = 'unknown'
            if add_dead_video(conn, video_id):
                dead += 1
        if 'channel_id' not in video_record:
            video_record['channel_id'] = 'unknown'

        # everything that doesn't go into the videos table gets popped below
        # so the videos table insert query can be constructed
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
                # channel for it getting inserted first

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
            # Had they not failed, this check would have never been reached.
            if update_video(conn, video_record):
                updated += 1

        else:
            if add_video(conn, video_record):
                video_ids.append(video_id)
                inserted += 1

        all_timestamps.setdefault(video_id, [])
        for timestamp in timestamps:
            if timestamp not in all_timestamps[video_id]:
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
                            # todo this is thrown because sometimes the tag_id
                            # is not retrieved, due to a weird combo of
                            # escaping and the tag being an SQLITE keyword.
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
        # some loop. An atomic insertion of sorts

    conn.commit()
    conn.close()

    yield json.dumps(
        {"records_processed": rows_passed,
         "records_inserted": inserted,
         "records_updated": updated,
         "records_in_db": len(video_ids),
         "failed_records": failed,
         "dead_records": dead})
    
    logger.info('-'*100 + f'\nPopulating finished')


def mock_records(db_path: str):
    from datetime import datetime
    conn = sqlite_connection(db_path)
    add_channel(conn, 'UCgkzrMGEbZemPI4hkz0Z9Bw', 'Jujimufu')
    add_channel(conn, 'unknown')
    vid = {"title": "WORLD RECORD GRIP BRIAN SHAW (rare footage)",
           "channel_id": "UCgkzrMGEbZemPI4hkz0Z9Bw",
           "id": '9EarvZN3e0M'}
    unknown_vid = {"title": "unknown",
                   "channel_id": "unknown",
                   "id": 'unknown'}
    deleted_vid = {"title": "unknown",
                   "channel_id": "unknown",
                   "id": 'bB0nnxaFOgw'}
    add_video(conn, vid)
    add_video(conn, unknown_vid)
    add_video(conn, deleted_vid)
    add_dead_video(conn, deleted_vid['id'])
    add_failed_request(conn, '9EarvZN3e0M', 1)

    add_time(conn,
             datetime.strptime("Nov 19, 2018, 8:04:38 PM EST"[:-4],
                               '%b %d, %Y, %I:%M:%S %p'), '9EarvZN3e0M')
    add_time(conn,
             datetime.strptime("Nov 16, 2018, 8:53:37 AM EST"[:-4],
                               '%b %d, %Y, %I:%M:%S %p'), 'unknown')

    add_time(conn,
             datetime.strptime("Nov 15, 2018, 11:04:01 PM EST"[:-4],
                               '%b %d, %Y, %I:%M:%S %p'), deleted_vid['id'])
    conn.commit()
    conn.close()


if __name__ == '__main__':
    import cProfile
    from os.path import join
    test_dir = r'G:\test_dir'
    with open(join(test_dir, 'api_key'), 'r') as file:
        api_key = file.read().strip()
    DB_PATH = join(test_dir, 'yt.sqlite')
    auth = youtube.get_api_auth(api_key)
    setup_tables(DB_PATH, auth)
    # mock_records(DB_PATH)
    cProfile.run(
        r"insert_videos(DB_PATH, get_all_records(r'D:\Downloads'), auth)",
        r'C:\Users\Vladimir\Desktop\results.txt'
    )
    # insert_videos(DB_PATH, get_all_records(r'D:\Downloads'), auth)
