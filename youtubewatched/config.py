from datetime import timedelta

PORT = 5000

DB_NAME = 'yt.sqlite'

video_keys_and_columns = (
    'id', 'publishedAt',
    'channelId', 'title',
    'description',
    'channelTitle', 'tags',
    'categoryId',
    'defaultAudioLanguage',
    'duration',
    'viewCount',
    'likeCount',
    'dislikeCount',
    'commentCount',
    'relevantTopicIds',
    'actualStartTime'
)

video_parts_to_get = ','.join([
    "contentDetails",  # 2
    "id",  # 0
    "snippet",  # 2
    "statistics",  # 2
    "topicDetails",  # 2
    "liveStreamingDetails"  # 2
])

'''YouTube Takeout seems to return timestamps in local time,
but without a concrete timezone.
In case archives were downloaded in different parts of the world, the same 
timestamp may show up as multiple different ones. The below value is used by a 
function to eliminate/block such duplicates, though at a potential cost of 
removing very few legitimate timestamps as well'''
MAX_TIME_DIFFERENCE = timedelta(hours=25)
