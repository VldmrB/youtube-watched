import json
import logging
import sqlite3
import time
from datetime import datetime

import youtube
from config import video_keys_and_columns
from sql_utils import generate_insert_query, generate_unconditional_update_query
from sql_utils import execute_query
from topics import topics
from utils import get_final_key_paths, convert_duration, calculate_subpercentage

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
add_failed_request_query = generate_insert_query(
    'failed_requests_ids',
    columns=FAILED_REQUESTS_IDS_COLUMNS)
add_dead_video_query = generate_insert_query('dead_videos_ids',
                                             columns=DEAD_VIDEOS_IDS_COLUMNS,
                                             on_conflict_ignore=True)


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
                value = value.replace('T', ' ')[:value.find('.')]
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
                   channel_id: str, channel_name: str, verbose=False):
    if execute_query(conn,
                     '''UPDATE channels SET title = ?
                     WHERE id = ?''', (channel_name, channel_id)):
        if verbose:
            logger.info(f'Updated channel name; id# {channel_id}, '
                        f'name {channel_name!r}')
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
    timestamps = {}
    for timestamp_record in cur.fetchall():
        timestamps.setdefault(timestamp_record[0], [])
        timestamps[timestamp_record[0]].append(timestamp_record[1])

    cur.execute("""SELECT id FROM dead_videos_ids;""")
    dead_videos_ids = [dead_video[0] for dead_video in cur.fetchall()]
    cur.execute("""SELECT * FROM failed_requests_ids;""")
    failed_requests_ids = {k: v for k, v in cur.fetchall()}
    cur.close()
    logger.info(f'\nStarting records\' insertion...\n' + '-'*100)

    # due to its made up ID, the unknown record is best handled manually
    if 'unknown' in records:
        records_passed += 1
        unknown_record = records.pop('unknown')
        unknown_record['id'] = 'unknown'
        unknown_record['title'] = 'unknown'
        unknown_record['channel_id'] = 'unknown'
        unknown_record['status'] = 'inactive'
        unknown_timestamps = unknown_record.pop('timestamps')
        timestamps.setdefault('unknown', [])
        if 'unknown' not in channels:
            add_channel(conn, 'unknown', 'unknown', verbosity_level_2)
        if 'unknown' not in video_ids:
            add_video(conn, unknown_record, verbosity_level_2)
        for timestamp in unknown_timestamps:
            if timestamp not in timestamps['unknown']:
                add_time(conn, timestamp, 'unknown', verbosity_level_3)
                
    sub_percent, sub_percent_int = calculate_subpercentage(len(records))

    for video_id, record in records.items():
        records_passed += 1
        if records_passed % sub_percent_int == 0:
            if verbosity_level_1:
                print(f'Processing entry # {records_passed}')
            yield (records_passed // sub_percent) / 10
        record['id'] = video_id

        if video_id not in video_ids or video_id in failed_requests_ids:
            pass
        else:
            '''
            This block deals with Takeout records already in the table. There 
            should only be two reasons for it being triggered: 
                1. Updating timestamps for the record, in case the video has 
                been watched again.
                2. If an older Takeout file was added (out of order, that is) 
                and contains info which was not available in the Takeouts 
                already processed, which resulted in a video being added with 
                an unknown title, for example. That should only happen if the 
                video in question was still available when the older Takeout was 
                generated, but was deleted by the time the newer Takeout was.
            '''
            timestamps.setdefault(video_id, [])
            for timestamp in record.pop('timestamps'):
                if timestamp not in timestamps[video_id]:
                    add_time(conn, timestamp, video_id, verbosity_level_2)

            if video_id in dead_videos_ids and 'title' in record:
                # Older Takeout file which for some reason was added out of
                # order and which has info on a video that has been
                # deleted by the time newer Takeout containing entries for that
                # video has been generated
                if 'channel_id' in record:
                    if 'channel_title' in record:
                        channel_title = record.pop('channel_title')
                    else:
                        channel_title = None
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
        # will vary from row to row

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
            # key for video records

        if 'relevant_topic_ids' in record:
            topics = record.pop('relevant_topic_ids')
        else:
            topics = None
        if 'tags' in record:
            tags = record.pop('tags')
        else:
            tags = None

        current_timestamps = record.pop('timestamps')

        if video_id in video_ids:  # passing this check means the API request
            # has been successfully made on this pass, whereas previous
            # attempts have failed
            if update_video(conn, record, verbosity_level_1):
                updated += 1

        else:
            if add_video(conn, record, verbosity_level_2):
                video_ids.append(video_id)
                inserted += 1

        timestamps.setdefault(video_id, [])
        for timestamp in current_timestamps:
            if timestamp not in timestamps[video_id]:
                add_time(conn, timestamp, video_id)

        if tags:
            add_tags_to_table_and_video(conn, tags, video_id, existing_tags,
                                        verbose=verbosity_level_3)

        if topics:
            for topic in topics:
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
    yield json.dumps(results)

    logger.info(json.dumps(results, indent=4))
    logger.info('\n' + '-'*100 + f'\nPopulating finished')


def update_videos(conn: sqlite3.Connection, api_auth,
                  update_age_cutoff=24, verbosity=3):
    verbosity_level_1 = verbosity >= 1
    verbosity_level_2 = verbosity >= 2
    verbosity_level_3 = verbosity >= 3
    skipped = 0
    records_passed, updated, failed_api_requests = 0, 0, 0
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("""SELECT * FROM videos WHERE status != ? and id != ?;""",
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
            yield (records_passed // sub_percent)/10

        if (now - record['last_updated']).total_seconds() < update_age_cutoff:
            skipped += 1
            continue
        record = dict(record)
        video_id = record['id']

        for attempt in range(1, 6):
            api_response = youtube.get_video_info(video_id, api_auth)
            time.sleep(0.01*attempt**attempt)
            if api_response:
                if video_id in failed_requests_ids.keys():
                    delete_failed_request(conn, video_id)
                if api_response['items']:
                    api_video_data = wrangle_video_record(api_response['items'])
                    filtered_api_video_data = {}
                    if 'published_at' in api_video_data:
                        # always the same, but will compare as different due to
                        # the same value from the record being of datetime type
                        api_video_data.pop('published_at')
                    for key in api_video_data:
                        # in case the response is messed up and has empty/zero
                        # values. Not sure if possible, but being safe.
                        # As well, if a value is the same as the current one,
                        # it's removed, hopefully that's faster than rewriting
                        # the fields with the same values
                        val = api_video_data[key]
                        if not val or val == record.get(key):
                            pass
                        else:
                            filtered_api_video_data[key] = val
                    record.update(filtered_api_video_data)
                else:
                    record['status'] = 'inactive'
                record['last_updated'] = datetime.utcnow().replace(
                    microsecond=0)
                break
        else:
            failed_requests_ids.setdefault(video_id, 0)
            attempts = failed_requests_ids[video_id]
            if attempts + 1 > 2:
                record['status'] = 'inactive'
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
            if channel_title != channels[channel_id]:
                update_channel(
                    conn, channel_id, channel_title, verbosity_level_1)
        if 'relevant_topic_ids' in record:
            topics = record.pop('relevant_topic_ids')
            for topic in topics:
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

    results = {"records_processed": records_passed,
               "records_updated": updated,
               "failed_api_requests": failed_api_requests}
    yield json.dumps(results)

    logger.info(json.dumps(results, indent=4))
    logger.info('\n' + '-'*100 + f'\nUpdating finished')
    print('Skipped', skipped, 'records out of', len(records))
