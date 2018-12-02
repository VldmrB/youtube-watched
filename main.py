import os
import json
import sqlite3
import time
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from config import DEVELOPER_KEY, WORK_DIR, DB_PATH
from ktools import utils
from ktools import fs

logger = fs.logger_obj(os.path.join(WORK_DIR, 'logs', 'fail.log'))

CLIENT_SECRETS_FILE = "client_secret.json"
SCOPES = ['https://www.googleapis.com/auth/youtube.readonly']
API_SERVICE_NAME = 'youtube'
YOUTUBE_API_VERSION = API_VERSION = 'v3'
YOUTUBE_API_SERVICE_NAME = 'youtube'

get = ','.join(list({  # integers are query costs, as of Nov 2018
                        "contentDetails": 2,
                        "id": 0,
                        "player": 0,
                        "snippet": 2,
                        "statistics": 2,
                        "status": 2,
                        "topicDetails": 2
                    }.keys()))


def get_oauth():
    flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE,
                                                     SCOPES)
    credentials = flow.run_console()
    return build(API_SERVICE_NAME, API_VERSION, credentials=credentials)


def get_api_auth():
    return build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,
                 developerKey=DEVELOPER_KEY)


def get_video_info(video_id, api_auth):
    try:
        results = api_auth.videos().list(id=video_id, part=get).execute()
        return results
    except:
        error = utils.err_display()
        logger.error(f'failed to retrieve video under ID {video_id}; '
                     f'error {error.err}')
        return


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


def get_all_videos_info():
    api_auth = get_api_auth()
    conn = sqlite3.connect(DB_PATH)
    # conn.row_factory = sqlite3.Row

    # watch_url = 'https://www.youtube.com/watch?v='
    count = 0
    logger.info('-'*10 + 'Starting video info retrieval' + '-'*10)
    cur = conn.cursor()
    cur.execute("""SELECT * FROM youtube_video_ids""")
    ids = cur.fetchall()
    cur.close()
    for row in ids:
        video_id = row[0]
        print(video_id)
        result = get_video_info(video_id, api_auth)
        if result:
            cur = conn.cursor()
            json_string = json.dumps(result)
            cur.execute("""INSERT INTO youtube_videos_info values (?, ?)""",
                        (video_id, json_string))
            conn.commit()
            cur.close()
        else:
            count += 1
        time.sleep(0.01)

    conn.close()
    logger.info('-'*10 + 'Video info retrieval finished' + '-'*10)
    logger.info(f'Total fails: {count}')


def get_categories_info():
    from pprint import pprint
    categories_json = get_api_auth().videoCategories().list(
        part='snippet', regionCode='US').execute()
    categories_json_path = os.path.join(WORK_DIR, 'categories.json')
    try:
        with open(categories_json_path, 'w') as file:
            json.dump(categories_json, file, indent=4)
        os.startfile(categories_json_path)
    except Exception:
        pprint(categories_json)
