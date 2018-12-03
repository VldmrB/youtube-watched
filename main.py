import os
import json
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from config import DEVELOPER_KEY, WORK_DIR
from ktools import utils, fs

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
    except Exception:
        error = utils.err_display()
        logger.error(f'failed to retrieve video under ID {video_id}; '
                     f'error {error.err}')
        return


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
