import json
import logging
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from config import video_parts_to_get
from config import DEVELOPER_KEY

logger = logging.getLogger(__name__)
logger.addFilter(logging.Filter(__name__))
loggers_to_disable = ['urllib', 'rsa', 'requests',
                      'pyasnl', 'oauthlib', 'google']
logging.Logger.manager: logging.Manager
for lgr_record in logging.Logger.manager.loggerDict:
    for lgr in loggers_to_disable:
        if lgr_record.startswith(lgr):
            logging.getLogger(lgr_record).setLevel(100)
            break


YOUTUBE_API_VERSION = 'v3'
YOUTUBE_API_SERVICE_NAME = 'youtube'


def get_api_auth(developer_key=DEVELOPER_KEY):
    if not developer_key:
        raise ValueError('Please provide an API key.')
    return build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,
                 developerKey=developer_key)


def get_video_info(video_id, api_auth):
    try:
        results = api_auth.videos().list(id=video_id,
                                         part=video_parts_to_get).execute()
        return results
    except HttpError as e:
        err_inf = json.loads(e.content)['error']
        reason = err_inf['errors'][0]['reason']
        if reason == 'keyInvalid':
            raise SystemExit('Invalid API key')
        logger.error(f'ID {video_id} retrieval failed,\n'
                     f'error code: ' + str(err_inf['code']) +
                     '\ndescription: ' + err_inf['message'] +
                     '\nreason: ' + reason)
        return False


def get_categories():
    api_auth = get_api_auth()
    try:
        return api_auth.videoCategories().list(part='snippet',
                                               regionCode='US').execute()
    except HttpError as e:
        err_inf = json.loads(e.content)['error']
        reason = err_inf['errors'][0]['reason']
        if reason == 'keyInvalid':
            raise SystemExit('Invalid API key')
        logger.error(f'Categories\' retrieval failed,\n'
                     f'error code: ' + str(err_inf['code']) +
                     '\ndescription: ' + err_inf['message'] +
                     '\nreason: ' + reason)
        return False


if __name__ == '__main__':

    from confidential import DEVELOPER_KEY
    result = get_categories()
    with open(r'G:\pyton\categories.json', 'w') as file:
        json.dump(result, file, indent=4)
