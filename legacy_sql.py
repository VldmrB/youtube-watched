import os
import sqlite3
import json
from config import DB_PATH, WORK_DIR
from ktools import fs


logger = fs.logger_obj(os.path.join(WORK_DIR, 'logs', 'fail.log'))


def populate_video_ids_in_sqlite():
    conn = sqlite3.connect(DB_PATH)

    # watch_url = 'https://www.youtube.com/watch?v='

    with open('video_ids.txt', 'r', newline='\r\n') as file:
        file = file.readlines()
    count = 0
    for line in file:
        count += 1
        stripped_line = line.strip()
        equal_index = stripped_line.find('=')
        video_id = stripped_line[equal_index+1:]
        duration_index = video_id.find('&t=')
        if duration_index > 1:
            video_id = video_id[:duration_index]
        cur = conn.cursor()
        cur.execute("""INSERT INTO youtube_video_ids values (?)""", (video_id,))
        conn.commit()
        cur.close()
    conn.close()
    print(count)


def populate_categories_into_sql():
    def bool_adapt(bool_value: bool): return str(bool_value)

    def bool_retrieve(bool_str): return eval(bool_str)

    sqlite3.register_adapter(bool, bool_adapt)
    sqlite3.register_converter('bool', bool_retrieve)
    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
    with open(os.path.join(WORK_DIR, 'categories.json'), 'r') as file:
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


def populate_parent_topics_into_sql():
    """Hard-coded table name/structure"""
    from topics import topics_by_category

    conn = sqlite3.connect(DB_PATH)

    for topic_dict in topics_by_category.values():
        for k, v in topic_dict.items():
            parent_topic_str = ' (parent topic)'
            if parent_topic_str in v:
                v = v.replace(parent_topic_str, '')
                print(k + ':', v)
                cur = conn.cursor()
                cur.execute('INSERT INTO parent_topics VALUES (?, ?)',
                            (k, v))
                conn.commit()
                cur.close()
            break
    conn.close()


def populate_sub_topics_into_sql():
    """Hard-coded table name/structure"""
    from topics import topics_by_category

    conn = sqlite3.connect(DB_PATH)

    for topic_dict in topics_by_category.values():
        parent_topic_str = ' (parent topic)'
        parent_topic_name = None
        for k, v in topic_dict.items():
            if parent_topic_str in v:
                parent_topic_name = k
                continue
            insert_tuple = (k, v, parent_topic_name)
            print(insert_tuple)
            cur = conn.cursor()
            cur.execute('INSERT INTO sub_topics VALUES (?, ?, ?)',
                        insert_tuple)
            conn.commit()
            cur.close()
    conn.close()
