import json
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
from pprint import pprint

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


def get_api_auth(developer_key):
    return build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,
                 developerKey=developer_key)


def get_video_info(video_id, api_auth):
    try:
        results = api_auth.videos().list(id=video_id, part=get,
                                         chart='mostPopular').execute()
        return results
    except HttpError as e:
        error_info = json.loads(e.content)
        return error_info


def get_categories_info(api_auth):
    try:
        categories_json = api_auth.videoCategories().list(
            part='snippet', regionCode='US').execute()
        return categories_json
    except HttpError as e:
        print('Couldn\'t retrieve categories\' info:')
        pprint(json.loads(e.resp)['error'])
