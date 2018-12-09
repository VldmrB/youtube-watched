import os

WORK_DIR = os.path.join(f'G:{os.sep}', 'pyton', 'youtube_watched_data')
LOG_DIR = os.path.join(WORK_DIR, 'logs')
DB_PATH = os.path.join(WORK_DIR, 'db.sqlite')

video_tags = ['id', 'publishedAt',
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
              'relevantTopicIds']
