import os

if os.path.exists('api_key'):
    with open('api_key', 'r') as file:
        DEVELOPER_KEY = file.read()
        if not DEVELOPER_KEY:
            DEVELOPER_KEY = None
else:
    DEVELOPER_KEY = None

video_keys_and_columns = ('id', 'publishedAt',
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
                          'relevantTopicIds')

video_parts_to_get = ','.join([
    "contentDetails",  # 2
    "id",  # 0
    "player",  # 0
    "snippet",  # 2
    "statistics",  # 2
    "status",  # 2
    "topicDetails"  # 2
])
