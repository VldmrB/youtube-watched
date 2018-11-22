import json
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
from pprint import pprint
from config import DEVELOPER_KEY
from ktools import utils
from ktools.env import *

# The CLIENT_SECRETS_FILE variable specifies the name of a file that contains
# the OAuth 2.0 information for this application, including its client_id and
# client_secret.
CLIENT_SECRETS_FILE = "client_secret.json"

SCOPES = ['https://www.googleapis.com/auth/youtube.readonly']
API_SERVICE_NAME = 'youtube'
YOUTUBE_API_VERSION = API_VERSION = 'v3'
YOUTUBE_API_SERVICE_NAME = 'youtube'


def get_authenticated_service():
    flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE,
                                                     SCOPES)
    credentials = flow.run_console()
    return build(API_SERVICE_NAME, API_VERSION, credentials=credentials)


def get_my_uploads_list(youtube):
    # Retrieve the contentDetails part of the channel resource for the
    # authenticated user's channel.
    channels_response = youtube.channels().list(
        mine=True,
        part='contentDetails'
    ).execute()

    for channel in channels_response['items']:
        # From the API response, extract the playlist ID that identifies list
        # of videos uploaded to the authenticated user's channel.
        return channel['contentDetails']['relatedPlaylists']['likes']

    return None


def list_my_uploaded_videos(uploads_playlist_id, youtube):
    # Retrieve the list of videos uploaded to the authenticated user's channel.
    playlistitems_list_request = youtube.playlistItems().list(
        playlistId=uploads_playlist_id,
        part='snippet',
        maxResults=50
    )

    print('Videos in list %s' % uploads_playlist_id)
    while playlistitems_list_request:
        playlistitems_list_response = playlistitems_list_request.execute()

        # print(information about each video.)
        for playlist_item in playlistitems_list_response['items']:
            title = playlist_item['snippet']['title']
            video_id = playlist_item['snippet']['resourceId']['videoId']
            print('%s (%s)' % (title, video_id))

        playlistitems_list_request = youtube.playlistItems().list_next(
            playlistitems_list_request, playlistitems_list_response)


def user_auth_and_retrieve():
    youtube = get_authenticated_service()
    try:
        uploads_playlist_id = get_my_uploads_list(youtube)
        print(uploads_playlist_id)
        if uploads_playlist_id:
            list_my_uploaded_videos(uploads_playlist_id, youtube)
        else:
            print('There is no uploaded videos playlist for this user.')
    except HttpError as e:
        print('An HTTP error %d occurred:\n%s' % (e.resp.status, e.content))

    #  YouTube User ID: YqpC6DMwWZMG9_jofDXtFA
    # YouTube Channel ID: UCYqpC6DMwWZMG9_jofDXtFA


def get_video_info(video_id=None, url=None, title=None):
    get = ','.join(list({
                "contentDetails": 2,
                "id": 0,
                "player": 0,
                "snippet": 2,
                "statistics": 2,
                "status": 2,
                "topicDetails": 2}.keys()))
    youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,
                    developerKey=DEVELOPER_KEY)

    results = youtube.videos().list(id=video_id, part=get).execute()
    pprint(results)
    if isinstance(results, dict):
        with open(DESKTOP_PATH + r'\video_info.json', 'w') as file:
            json.dump(results, file, indent=4)

    else:
        try:
            import shelve
            with shelve.open(DESKTOP_PATH + r'\video_info') as file:
                file['the_only_object'] = results
        except:
            utils.err_display()

    return youtube


# get_video_info('XcTBke6kwKI')
