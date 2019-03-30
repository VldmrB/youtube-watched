import bisect
import json
import logging
import itertools
import sqlite3
import time
from datetime import datetime
from typing import Union

import youtube
from config import video_keys_and_columns, MAX_TIME_DIFFERENCE
from topics import topics
from utils.sql import execute_query
from utils.sql import generate_insert_query, generate_unconditional_update_query
from utils.gen import (are_different_timestamps, timestamp_is_unique_in_list,
                       remove_timestamps_from_one_list_from_another)

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

    'topics': '''topics (
    id text primary key,
    topic text
    );''',

    'tags': '''tags (
    id integer primary key,
    tag text unique
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
    stream text,
    foreign key (channel_id) references channels (id)
    on update cascade on delete cascade
    );''',

    'videos_tags': '''videos_tags (
    video_id text,
    tag_id int,
    unique (video_id, tag_id),
    foreign key (video_id) references videos (id)
    on update cascade on delete cascade,
    foreign key (tag_id) references tags (id)
    on update cascade on delete cascade
    );''',

    'videos_topics': '''videos_topics (
    video_id text,
    topic_id text,
    unique (video_id, topic_id),
    foreign key (video_id) references videos (id)
    on update cascade on delete cascade
    );''',
    
    'videos_timestamps': '''videos_timestamps (
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
TOPICS_COLUMNS = ['id', 'topic']
TAGS_COLUMNS = ['tag']  # id column value is added implicitly by SQLite
VIDEOS_TAGS_COLUMNS = ['video_id', 'tag_id']
VIDEOS_TOPICS_COLUMNS = ['video_id', 'topic_id']
VIDEOS_TIMESTAMPS_COLUMNS = ['video_id', 'watched_at']
FAILED_REQUESTS_IDS_COLUMNS = ['id', 'attempts']
DEAD_VIDEOS_IDS_COLUMNS = ['id']

# below are rigid insert queries, ones whose amount of columns will not change
# between records
# add_channel and add_video are compiled every run due to dynamic col amount
add_tag_query = generate_insert_query('tags', columns=TAGS_COLUMNS)
add_tag_to_video_query = generate_insert_query('videos_tags',
                                               columns=VIDEOS_TAGS_COLUMNS)
add_topic_to_video_query = generate_insert_query('videos_topics',
                                                 columns=VIDEOS_TOPICS_COLUMNS)
add_time_to_video_query = generate_insert_query(
    'videos_timestamps',
    columns=VIDEOS_TIMESTAMPS_COLUMNS)
delete_time_query = '''DELETE FROM videos_timestamps
                       WHERE video_id = ? AND watched_at = ?'''
add_failed_request_query = generate_insert_query(
    'failed_requests_ids',
    columns=FAILED_REQUESTS_IDS_COLUMNS)
add_dead_video_query = generate_insert_query('dead_videos_ids',
                                             columns=DEAD_VIDEOS_IDS_COLUMNS,
                                             on_conflict_ignore=True)


def get_final_key_paths(
        obj: Union[dict, list, tuple], cur_path: str = '',
        append_values: bool = False,
        paths: list = None, black_list: list = None,
        final_keys_only: bool = False):
    """
    Returns Python ready, full key paths as strings

    :param obj:
    :param cur_path: name of the variable that's being passed as the obj can be
    passed here to create eval ready key paths
    :param append_values: return corresponding key values along with the keys
    :param paths: the list that will contain all the found key paths, no need
    to pass anything
    :param black_list: dictionary keys which will be ignored
    :param final_keys_only: return only the final key from each path
    :return:
    """
    if paths is None:
        paths = []

    if isinstance(obj, (dict, list, tuple)):
        if isinstance(obj, dict):
            for key in obj:
                new_path = cur_path + f'[\'{key}\']'
                if isinstance(obj[key], dict):
                    if black_list is not None and key in black_list:
                        continue
                    get_final_key_paths(
                        obj[key], new_path, append_values, paths, black_list,
                        final_keys_only)
                elif isinstance(obj[key], (list, tuple)):
                    get_final_key_paths(
                        obj[key], new_path, append_values, paths, black_list,
                        final_keys_only)
                else:
                    if final_keys_only:
                        last_bracket = new_path.rfind('[\'')
                        new_path = new_path[
                                   last_bracket+2:new_path.rfind('\'')]
                    if append_values:
                        to_append = [new_path, obj[key]]
                    else:
                        to_append = new_path
                    paths.append(to_append)
        else:
            key_added = False  # same as in get_final_keys function
            for i in range(len(obj)):
                if isinstance(obj[i], (dict, tuple, list)):
                    get_final_key_paths(
                        obj[i], cur_path + f'[{i}]', append_values,
                        paths, black_list, final_keys_only)
                else:
                    if not key_added:
                        if final_keys_only:
                            last_bracket = cur_path.rfind('[\'')
                            cur_path = cur_path[
                                       last_bracket+2:cur_path.rfind('\'')]
                        if append_values:
                            to_append = [cur_path, obj]
                        else:
                            to_append = cur_path
                        paths.append(to_append)
                        key_added = True

    return paths


def convert_duration(duration_iso8601: str):
    duration = duration_iso8601.split('T')
    duration = {'P': duration[0][1:], 'T': duration[1]}
    int_value = 0
    for key, value in duration.items():
        new_value = ''
        if not value:
            continue
        for element in value:
            if element.isnumeric():
                new_value += element
            else:
                new_value += element + ' '
        split_vals = new_value.strip().split(' ')
        for val in split_vals:
            if val[-1] == 'Y':
                int_value += int(val[:-1]) * 31_536_000
            elif val[-1] == 'M':
                if key == 'P':
                    int_value += int(val[:-1]) * 2_592_000
                else:
                    int_value += int(val[:-1]) * 60
            elif val[-1] == 'W':
                int_value += int(val[:-1]) * 604800
            elif val[-1] == 'D':
                int_value += int(val[:-1]) * 86400
            elif val[-1] == 'H':
                int_value += int(val[:-1]) * 3600
            elif val[-1] == 'S':
                int_value += int(val[:-1])

    return int_value


def calculate_subpercentage(records_amount: int):
    total_records = records_amount
    if total_records >= 1000:
        sub_percent = total_records / 1000
    elif total_records >= 100:
        sub_percent = total_records / 100
    elif total_records >= 10:
        sub_percent = total_records / 10
    else:
        sub_percent = total_records
    return sub_percent, int(sub_percent)


def wrangle_video_record(json_obj: dict):
    """
    Extracts the keys deemed necessary from a Youtube API response, converts
    some of them to the right types and returns them in a dict
    """
    entry_dict = {}
    for key, value in get_final_key_paths(
            json_obj, '', True, black_list=['localized', 'thumbnails'],
            final_keys_only=True):
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
                value = value.replace('T', ' ')
            elif key == 'actual_start_time':
                key = 'stream'
                value = 'true'
            elif key in ['view_count', 'dislike_count', 'like_count',
                         'comment_count']:
                value = int(value)
            entry_dict[key] = value

    return entry_dict


def drop_dynamic_tables(conn: sqlite3.Connection):
    for table in TABLE_SCHEMAS:
        if table not in ['categories', 'topics', 'parent_topics',
                         'dead_videos_ids']:
            execute_query(conn, '''DROP TABLE IF EXISTS ''' + table)
    conn.commit()


def add_channel(conn: sqlite3.Connection, channel_id: str,
                channel_name: str = None, verbose=False) -> bool:
    values = [channel_id]
    if channel_name:
        values.append(channel_name)
    query_string = generate_insert_query('channels',
                                         CHANNEL_COLUMNS[:len(values)],
                                         on_conflict_ignore=True)
    if execute_query(conn, query_string, tuple(values)):
        if verbose:
            logger.info(f'Added channel; id# {channel_id}, '
                        f'name {channel_name!r}')
        return True


def update_channel(conn: sqlite3.Connection,
                   channel_id: str, channel_name: str, old_name: str,
                   verbose=False):
    if execute_query(conn,
                     '''UPDATE channels SET title = ?
                     WHERE id = ?''', (channel_name, channel_id)):
        if verbose:
            logger.info(f'Updated channel name; id# {channel_id}, '
                        f'from {old_name!r} to {channel_name!r}')
        return True


def add_video(conn: sqlite3.Connection, cols_vals: dict, verbose=False):
    query_string = generate_insert_query('videos', list(cols_vals.keys()))
    values = cols_vals.values()
    if execute_query(conn, query_string, tuple(values)):
        if verbose:
            logger.info(f'Added video; id# {cols_vals["id"]}, '
                        f'title {cols_vals.get("title")!r}')
        return True


def update_video(conn: sqlite3.Connection, cols_vals: dict, verbose=False):
    """Updates all the fields that are passed"""
    video_id = cols_vals.pop('id')
    query_string = generate_unconditional_update_query(
        'videos', list(cols_vals.keys()))
    values = list(cols_vals.values())
    values.append(video_id)
    if execute_query(conn, query_string, tuple(values)):
        if verbose:
            logger.info(f'Updated video {video_id!r}')
        return True


def add_tag(conn: sqlite3.Connection, tag: str, verbose=False):
    if execute_query(conn, add_tag_query, (tag,)):
        if verbose:
            logger.info(f'Added tag {tag!r}')
        return True


def add_tag_to_video(conn: sqlite3.Connection, tag_id: int, video_id: str,
                     verbose=False):
    if execute_query(conn, add_tag_to_video_query, (video_id, tag_id)):
        if verbose:
            logger.info(f'Added tag id# {tag_id} to {video_id!r}')
        return True


def add_tags_to_table_and_video(conn: sqlite3.Connection, tags: list,
                                video_id: str, existing_tags: dict,
                                existing_videos_tags_records: dict = None,
                                verbose=False):

    id_query_string = 'SELECT id FROM tags WHERE tag = ?'
    for tag in tags:
        if tag not in existing_tags:
            tag_id = None
            if add_tag(conn, tag):
                if verbose:
                    logger.info(f'Added tag {tag!r}')
                tag_id = execute_query(conn, id_query_string, (tag,))[0][0]
                existing_tags[tag] = tag_id

        else:
            tag_id = existing_tags[tag]
        if tag_id:
            if existing_videos_tags_records:
                if existing_videos_tags_records.get(video_id):
                    if tag_id not in existing_videos_tags_records[video_id]:
                        if add_tag_to_video(conn, tag_id, video_id) and verbose:
                            logger.info(f'Added {tag!r} to {video_id!r}')
            else:
                if add_tag_to_video(conn, tag_id, video_id) and verbose:
                    # duplicate tags are possible in a record, but happen
                    # rarely and are allowed to throw some integrity errors
                    # (caught and logged)
                    logger.info(f'Added {tag!r} to {video_id!r}')


def add_topic_to_video(conn: sqlite3.Connection, topic: str, video_id: str,
                       verbose=False):
    if execute_query(conn, add_topic_to_video_query, (video_id, topic)):
        if verbose:
            logger.info(f'Added topic {topic!r} to {video_id!r}')
        return True


def add_time(conn: sqlite3.Connection, watched_at: str, video_id: str,
             verbose=False):
    if execute_query(
            conn, add_time_to_video_query, (video_id, watched_at), False):
        if verbose:
            logger.info(f'Added timestamp {watched_at} to {video_id!r}')
        return True


def delete_time(conn: sqlite3.Connection, watched_at: str, video_id: str,
                verbose=False):
    if execute_query(conn, delete_time_query, (video_id, watched_at)):
        if verbose:
            logger.info(f'Removed timestamp {watched_at} from {video_id!r}')
        return True


def add_failed_request(conn: sqlite3.Connection, video_id: str, attempts: int,
                       verbose=False):
    if attempts == 1:
        if execute_query(conn, add_failed_request_query, (video_id, 1)):
            if verbose:
                logger.info(f'Failed {attempts} cycle of attempts to request '
                            f'{video_id!r} info from the API')
            return True
    else:
        if execute_query(
                conn,
                '''UPDATE failed_requests_ids SET attempts = ? WHERE id = ?''',
                (attempts, video_id)):
            if verbose:
                logger.info(f'Failed {attempts} cycles of attempts to request '
                            f'{video_id!r} info from the API')
            return True


def delete_failed_request(conn: sqlite3.Connection, video_id):
    return execute_query(conn,
                         '''DELETE FROM failed_requests_ids
                         WHERE id = ?''',
                         (video_id,))


def add_dead_video(conn: sqlite3.Connection, video_id):
    return execute_query(conn, add_dead_video_query, (video_id,))


def delete_dead_video(conn: sqlite3.Connection, video_id, verbose=False):
    if execute_query(
            conn, '''DELETE FROM dead_videos_ids WHERE id = ?''', (video_id,)):
        if verbose:
            logger.info(f'Removed {video_id!r} from dead videos due to '
                        f'retrieving some identifying info for the record')
        return True


def insert_or_refresh_categories(conn: sqlite3.Connection, api_auth,
                                 refresh: bool = True):
    categories = youtube.get_categories(api_auth)
    query_string = generate_insert_query('categories',
                                         columns=CATEGORIES_COLUMNS,
                                         on_conflict_ignore=True)
    if refresh or not execute_query(conn, 'SELECT * FROM categories'):
        if execute_query(conn, 'DELETE FROM categories WHERE id IS NOT NULL;'):
            for category_dict in categories['items']:
                id_ = category_dict['id']
                channel_id = category_dict['snippet']['channelId']
                title = category_dict['snippet']['title']
                assignable = str(category_dict['snippet']['assignable'])
                etag = category_dict['etag']
                execute_query(conn, query_string, (id_, channel_id, title,
                                                   assignable, etag))
        conn.commit()


def insert_topics(conn: sqlite3.Connection):
    query_string = generate_insert_query('topics', columns=TOPICS_COLUMNS,
                                         on_conflict_ignore=True)
    for k, v in topics.items():
        insert_tuple = (k, v)
        execute_query(conn, query_string, insert_tuple)

    conn.commit()


def setup_tables(conn: sqlite3.Connection, api_auth):
    for schema in TABLE_SCHEMAS:
        create_schema_ = 'CREATE TABLE IF NOT EXISTS ' + TABLE_SCHEMAS[schema]
        execute_query(conn, create_schema_)

    insert_or_refresh_categories(conn, api_auth, False)
    insert_topics(conn)

    conn.commit()


def insert_videos(conn, records: dict, api_auth, verbosity=1):
    verbosity_level_1 = verbosity >= 1
    verbosity_level_2 = verbosity >= 2
    verbosity_level_3 = verbosity >= 3
    records_passed, inserted, updated, failed_api_requests, dead = 0, 0, 0, 0, 0
    cur = conn.cursor()
    cur.execute("""SELECT id FROM videos;""")
    video_ids = [row[0] for row in cur.fetchall()]
    cur.execute("""SELECT id FROM channels;""")
    channels = [row[0] for row in cur.fetchall()]
    cur.execute("""SELECT * FROM tags;""")
    existing_tags = {v: k for k, v in cur.fetchall()}
    cur.execute("""SELECT * FROM videos_timestamps;""")
    db_timestamps = {}
    for timestamp_record in cur.fetchall():
        db_timestamps.setdefault(timestamp_record[0], [])
        db_timestamps[timestamp_record[0]].append(timestamp_record[1])
    cur.execute("""SELECT id FROM dead_videos_ids;""")
    dead_videos_ids = [dead_video[0] for dead_video in cur.fetchall()]
    cur.execute("""SELECT * FROM failed_requests_ids;""")
    failed_requests_ids = {k: v for k, v in cur.fetchall()}
    cur.close()
    logger.info(f'\nStarting records\' insertion...\n' + '-'*100)

    # due to its made up ID, the unknown record is best handled outside the loop
    unknown_record = records.pop('unknown', None)
    unk_db_timestamps = db_timestamps.setdefault('unknown', [])
    if unknown_record:
        unknown_record['id'] = 'unknown'
        unknown_record['title'] = 'unknown'
        unknown_record['channel_id'] = 'unknown'
        unknown_record['status'] = 'inactive'
        unknown_timestamps = unknown_record.pop('timestamps')
        # clean possible new unknowns of known ones already in the database
        all_known_timestamps_ids = list(db_timestamps.keys())
        all_known_timestamps_ids.remove('unknown')
        all_known_timestamps_lists = [i for i in
                                      [db_timestamps[v_id]
                                       for v_id in all_known_timestamps_ids]]
        all_known_timestamps = list(itertools.chain.from_iterable(
            all_known_timestamps_lists))
        remove_timestamps_from_one_list_from_another(all_known_timestamps,
                                                     unknown_timestamps)
        if 'unknown' not in channels:
            add_channel(conn, 'unknown', 'unknown', verbosity_level_2)
        if 'unknown' not in video_ids:
            add_video(conn, unknown_record, verbosity_level_2)
            inserted += 1
        for candidate in unknown_timestamps:
            if timestamp_is_unique_in_list(candidate, unk_db_timestamps):
                add_time(conn, candidate, 'unknown', verbosity_level_3)

    def add_known_timestamps_and_remove_from_unknown(new_timestamps):
        db_timestamps.setdefault(video_id, [])
        db_timestamps[video_id].sort()
        added_timestamps = []
        for new in new_timestamps:
            if timestamp_is_unique_in_list(new,
                                           db_timestamps[video_id]):
                add_time(conn, new, video_id, verbosity_level_2)
                added_timestamps.append(new)
        # clean db unknown timestamps of ones that are now known
        for db_incumbent in added_timestamps:
            start = bisect.bisect_left(unk_db_timestamps,
                                       db_incumbent - MAX_TIME_DIFFERENCE)
            end = bisect.bisect_right(unk_db_timestamps,
                                      db_incumbent + MAX_TIME_DIFFERENCE)
            if start != end:
                for unk_incumbent in range(start, end):
                    if not are_different_timestamps(
                            db_incumbent, unk_db_timestamps[unk_incumbent]):
                        delete_time(conn,
                                    unk_db_timestamps.pop(unk_incumbent),
                                    'unknown', verbose=verbosity_level_1)
                        break

    sub_percent, sub_percent_int = calculate_subpercentage(len(records))

    for video_id, record in records.items():
        records_passed += 1
        if records_passed % sub_percent_int == 0:
            if verbosity_level_1:
                print(f'Processing entry # {records_passed}')
            yield ((records_passed // sub_percent) / 10,
                   updated, failed_api_requests)
        record['id'] = video_id

        if video_id not in video_ids or video_id in failed_requests_ids:
            pass  # API attempt will be made further below
        else:
            '''
            This block deals with Takeout records already in the table. There 
            should only be two reasons for it being triggered: 
                1. Updating timestamps for the record, in case new ones are 
                found.
                2. If an older Takeout file was added (out of order, that is) 
                and contains info which was not available in the Takeouts 
                already processed, which resulted in a video being added with 
                an unknown title, for example. That should only happen if the 
                video in question was still available when the older Takeout was 
                generated, but was deleted by the time the newer Takeout was.
            '''
            add_known_timestamps_and_remove_from_unknown(
                record.pop('timestamps'))

            if video_id in dead_videos_ids and 'title' in record:
                # Older Takeout file which for some reason was added out of
                # order and which has info on a video that has been
                # deleted by the time newer Takeout containing entries for that
                # video has been generated
                if 'channel_id' in record:
                    channel_title = record.pop('channel_title', None)
                    add_channel(conn, record['channel_id'], channel_title,
                                verbosity_level_2)
                record['status'] = 'inactive'
                record['last_updated'] = datetime.utcnow(
                ).replace(microsecond=0)
                if update_video(conn, record, verbosity_level_2):
                    delete_dead_video(conn, video_id, verbosity_level_1)
                    updated += 1
            conn.commit()
            continue

        for attempt in range(1, 6):
            api_response = youtube.get_video_info(video_id, api_auth)
            time.sleep(0.01*attempt**attempt)
            if api_response:
                delete_failed_request(conn, video_id)  # there may not be a
                # record for this yet due to one only being created after 5
                # failed attempts, while this checks regardless of the number
                # of those, but that's fine
                if api_response['items']:
                    api_video_data = wrangle_video_record(api_response['items'])
                    record.update(api_video_data)
                    record['status'] = 'active'
                else:
                    record['status'] = 'inactive'

                record['last_updated'] = str(datetime.utcnow().replace(
                    microsecond=0))
                break
        else:
            failed_requests_ids.setdefault(video_id, 0)
            attempts = failed_requests_ids[video_id]
            if attempts + 1 > 2:
                record['status'] = 'inactive'
                record['last_updated'] = str(datetime.utcnow().replace(
                    microsecond=0))
                delete_failed_request(conn, video_id)
            else:
                if add_failed_request(conn, video_id, attempts + 1,
                                      verbosity_level_1):
                    failed_api_requests += 1
            # video is still inserted, but this will be run again a couple of
            # times; it will attempt to retrieve the data from API and
            # update the record accordingly, if any data is returned

        if 'title' not in record:
            record['title'] = 'unknown'
            if add_dead_video(conn, video_id):
                dead += 1
        if 'channel_id' not in record:
            record['channel_id'] = 'unknown'

        # everything that doesn't go into the videos table gets popped
        # so the videos table insert query can be constructed
        # automatically with the remaining items, since their amount
        # will vary from entry to entry

        if 'channel_title' in record:
            channel_title = record.pop('channel_title')
        else:
            channel_title = None
        channel_id = record['channel_id']

        if add_channel(conn, channel_id, channel_title, verbosity_level_2):
            channels.append(record['channel_id'])
        else:
            continue  # nothing else can/should be inserted without the
            # channel for it getting inserted first as channel_id is a foreign
            # key for video records the video id field from which is in turn a
            # foreign key for most other tables

        if 'relevant_topic_ids' in record:
            topics_list = record.pop('relevant_topic_ids')
        else:
            topics_list = None
        if 'tags' in record:
            tags = record.pop('tags')
        else:
            tags = None

        candidate_timestamps = record.pop('timestamps')

        if video_id in video_ids:
            # passing this check means the API request has been successfully
            # made during this run, whereas previous attempts have failed
            if update_video(conn, record, verbosity_level_1):
                updated += 1

        else:
            if add_video(conn, record, verbosity_level_2):
                video_ids.append(video_id)
                inserted += 1

        add_known_timestamps_and_remove_from_unknown(candidate_timestamps)

        if tags:
            add_tags_to_table_and_video(conn, tags, video_id, existing_tags,
                                        verbose=verbosity_level_3)

        if topics_list:
            for topic in topics_list:
                add_topic_to_video(conn, topic, video_id, verbosity_level_3)

        conn.commit()  # committing after every record ensures each record's
        # info is inserted fully, or not at all, in case of an unforeseen
        # failure during some loop. An atomic insertion, of sorts

    conn.commit()

    results = {"records_processed": records_passed,
               "records_inserted": inserted,
               "records_updated": updated,
               "records_in_db": len(video_ids),
               "failed_api_requests": failed_api_requests,
               "dead_records": dead}

    logger.info(json.dumps(results, indent=4))
    logger.info('\n' + '-'*100 + f'\nPopulating finished')


def update_videos(conn: sqlite3.Connection, api_auth,
                  update_age_cutoff=86400, verbosity=1):
    verbosity_level_1 = verbosity >= 1
    verbosity_level_2 = verbosity >= 2
    verbosity_level_3 = verbosity >= 3
    skipped = 0
    records_passed, updated, failed_api_requests, newly_inactive = 0, 0, 0, 0
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("""SELECT * FROM videos WHERE status != ? and id != ?
                   ORDER BY last_updated;""",
                ('inactive', 'unknown'))
    records = list(cur.fetchall())
    cur.execute("""SELECT * FROM channels WHERE title is not NULL;""")
    channels = {k: v for k, v in cur.fetchall()}
    cur.execute("""SELECT * FROM tags;""")
    existing_tags = {v: k for k, v in cur.fetchall()}
    cur.execute("""SELECT * FROM videos_tags""")
    existing_videos_tags = {}
    for video_tag_entry in cur.fetchall():
        existing_videos_tags.setdefault(video_tag_entry[0], [])
        existing_videos_tags[video_tag_entry[0]].append(video_tag_entry[1])
    cur.execute("""SELECT * FROM videos_topics""")
    existing_topics_tags = {}
    for video_topic_entry in cur.fetchall():
        existing_topics_tags.setdefault(video_topic_entry[0], [])
        existing_topics_tags[video_topic_entry[0]].append(video_topic_entry[1])
    cur.execute("""SELECT * FROM failed_requests_ids;""")
    failed_requests_ids = {k: v for k, v in cur.fetchall()}
    cur.close()
    now = datetime.utcnow()
    sub_percent, sub_percent_int = calculate_subpercentage(len(records))
    if verbosity >= 1:
        logger.info(f'\nStarting records\' updating...\n' + '-'*100)
    for record in records:
        records_passed += 1

        if records_passed % sub_percent_int == 0:
            if verbosity >= 1:
                print(f'Processing entry # {records_passed}')
            yield ((records_passed // sub_percent)/10, updated,
                   failed_api_requests, newly_inactive)
        last_updated_dtm = datetime.strptime(record['last_updated'],
                                             '%Y-%m-%d %H:%M:%S')
        if (now - last_updated_dtm).total_seconds() < update_age_cutoff:
            skipped += 1
            continue
        record = dict(record)
        video_id = record['id']

        for attempt in range(1, 6):
            api_response = youtube.get_video_info(video_id, api_auth)
            time.sleep(0.01*attempt**attempt)
            if api_response:  # if an exception is caught when getting this
                # value via the above function, the result returned is False
                if video_id in failed_requests_ids.keys():
                    delete_failed_request(conn, video_id)
                if api_response['items']:
                    api_video_data = wrangle_video_record(api_response['items'])
                    filtered_api_video_data = {}
                    api_video_data.pop('published_at', None)
                    # always the same, but will compare as different due to
                    # the same value from the record being of datetime type
                    for key in api_video_data:
                        # in case the response is messed up and has empty/zero
                        # values. Not sure if possible, but being safe.
                        # As well, if a value is the same as the current one,
                        # it's removed; hopefully that's faster than rewriting
                        # the fields with the same values
                        val = api_video_data[key]
                        if not val or val == record.get(key):
                            pass
                        else:
                            filtered_api_video_data[key] = val
                    record.update(filtered_api_video_data)
                else:
                    record['status'] = 'inactive'
                    newly_inactive += 1
                    logger.info(f'{record["id"]} is now inactive')
                record['last_updated'] = datetime.utcnow().replace(
                    microsecond=0)
                break
        else:
            failed_requests_ids.setdefault(video_id, 0)
            attempts = failed_requests_ids[video_id]
            if attempts + 1 > 2:
                record['status'] = 'inactive'
                newly_inactive += 1
                logger.info(f'{record["id"]} is now inactive')
                delete_failed_request(conn, video_id)
            else:
                if add_failed_request(
                        conn, video_id, attempts + 1, verbosity_level_1):
                    failed_api_requests += 1
            continue
        record['last_updated'] = datetime.utcnow().replace(microsecond=0)
        if 'tags' in record:
            tags = record.pop('tags')
            add_tags_to_table_and_video(conn, tags, video_id, existing_tags,
                                        existing_videos_tags, verbosity_level_3)
            # perhaps, the record should also be checked for tags that have
            # been removed from the updated version and have them removed from
            # the DB as well. However, keeping a fuller record, despite what
            # the video's uploader/author might think about its accuracy,
            # seems like a better option
        if 'channel_title' in record:
            channel_title = record.pop('channel_title')
            channel_id = record['channel_id']
            try:
                    if channel_title != channels[channel_id]:
                        update_channel(
                            conn, channel_id, channel_title,
                            channels[channel_id], verbosity_level_1)
                        channels[channel_id] = channel_title
            except KeyError:
                """The channel now has a different ID... it's a thing.
                One possible reason for this is large channels, belonging to 
                large media companies, getting split off into smaller
                channels. That's what it looked like when I came across it.
                
                Only encountered this once in ~19k of my own records."""
                add_channel(conn, channel_id, channel_title, verbosity_level_2)

            pass
        if 'relevant_topic_ids' in record:
            topics_list = record.pop('relevant_topic_ids')
            for topic in topics_list:
                if existing_topics_tags.get(video_id):
                    if topic not in existing_topics_tags[video_id]:
                        add_topic_to_video(conn, topic,
                                           video_id, verbosity_level_2)

        if update_video(conn, record):
            updated += 1

        conn.commit()

    conn.commit()
    execute_query(conn, 'VACUUM')
    conn.row_factory = None

    results = {'records_processed': records_passed,
               'records_updated': updated,
               'failed_api_requests': failed_api_requests,
               'newly_inactive': newly_inactive}

    logger.info(json.dumps(results, indent=4))
    logger.info('\n' + '-'*100 + f'\nUpdating finished')
    print('Skipped', skipped, 'records out of', len(records))
