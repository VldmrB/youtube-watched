from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, DateTime, Text


Base = declarative_base()


class Video(Base):
    __tablename__ = 'videos'

    id = Column(Text, primary_key=True)
    published_at = Column(DateTime)
    watched_on = Column(DateTime)
    channel_id = Column(Text)
    title = Column(Text)
    description = Column(Text)
    category_id = Column(Text)
    default_audio_language = Column(Text)
    duration = Column(Integer)
    view_count = Column(Integer)
    like_count = Column(Integer)
    dislike_count = Column(Integer)
    comment_count = Column(Integer)

    def __repr__(self):
        for k, v in self.__dict__.items():
            if k != '_sa_instance_state':
                print(k + ':', v)


# videos_table = Table('videos', meta,
#
#                      Column('id', Text,  primary_key=True),
#                      Column('published_at', DateTime),
#                      Column('watched_on', DateTime),
#                      Column('channel_id', Text),
#                      Column('title', Text),
#                      Column('description', Text),
#                      Column('category_id', Text),
#                      Column('default_audio_language', Text),
#                      Column('duration', Integer),
#                      Column('view_count', Integer),
#                      Column('like_count', Integer),
#                      Column('dislike_count', Integer),
#                      Column('comment_count', Integer),
#                      )
