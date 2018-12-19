import json
import logging
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
from config import parts_to_get

logger = logging.getLogger(__name__)
logger.addFilter(logging.Filter(__name__))
disable_loggers = ['urllib', 'rsa', 'requests', 'pyasnl', 'oauthlib', 'google']

logging.Logger.manager: logging.Manager
for lgr in logging.Logger.manager.loggerDict:
    for disable_lgr in disable_loggers:
        if lgr.startswith(disable_lgr):
            logging.getLogger(lgr).setLevel(100)
            break


CLIENT_SECRETS_FILE = "client_secret.json"
SCOPES = ['https://www.googleapis.com/auth/youtube.readonly']
API_SERVICE_NAME = 'youtube'
YOUTUBE_API_VERSION = API_VERSION = 'v3'
YOUTUBE_API_SERVICE_NAME = 'youtube'


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
        results = api_auth.videos().list(id=video_id, part=parts_to_get,
                                         chart='mostPopular').execute()
        return results
    except HttpError as e:
        err_inf = json.loads(e.content)['error']
        logger.error(f'ID {video_id} retrieval failed,\n'
                     f'error code: ' + str(err_inf['code']) +
                     '\ndescription: ' + err_inf['message'])
        return err_inf


def get_categories_info(api_auth):
    try:
        return api_auth.videoCategories().list(
            part='snippet', regionCode='US').execute()
    except HttpError as e:
        err_inf = json.loads(e.content)['error']
        logger.error(f'Categories\' info retrieval failed,\n'
                     f'error code: ' + str(err_inf['code']) +
                     '\ndescription: ' + err_inf['message'])
