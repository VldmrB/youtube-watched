import os
import logging
# from pprint import pprint
from confidential import DEVELOPER_KEY
import youtube
from utils import logging_config

developer_key = DEVELOPER_KEY
if not developer_key:
    raise SystemExit(
        'An API key must be assigned to developer_key in execute.py '
        'to retrieve video info.')

if __name__ == '__main__':
    log_path = r'C:\Users\Vladimir\Desktop\fails.log'
    logging_config(log_path, __name__)
    logger = logging.getLogger(__name__)
    logger.info('Start logging...')
    res = youtube.get_video_info('___________',
                                 api_auth=youtube.get_api_auth(DEVELOPER_KEY))
    logger.info('End logging...')
    os.startfile(log_path)


def insert_video_into_sql():
    pass
    """
    A somewhat adapted version of a combo the current construct SQL insert 
    query and actually inserting the record into the videos table.
    This would be stored in another module.
    """


def insert_videos_into_sql():
    pass

    """
    Start logging
    
    Loop over a provided list of ids (generated from Takeout data), 
    passing each to a function responsible for retrieving an individual record 
    
    Then, depending on whether the request is successful or returns an error: 
        if successful:
            if not empty:
                pass to a function responsible for constructing an SQL query 
                and inserting the result into the DB (could be two functions)
            else:
                same as above, but set id to unknown (or removed?)
        else:
            log the error reason and ID of the video
            insert ID into a separate table for failed requests
    
    """


def attempt_insert_failed_videos_into_sql():
    pass

    """
    Check if the table is not empty. If empty, return.
    Recover list of IDs from the table with IDs which failed to be retrieved 
    earlier for temporary reasons and pass them to insert_videos_into_sql for 
    another attempt.
    After 2-5 (TBD) unsuccessful attempts, those records would be purged from 
    the table
    """


def create_project_dir(path: str):
    """
    Where all the data is stored. Must be the first thing to be run, if using
    CLI.
    """
    os.makedirs(path, exist_ok=True)
    os.chdir(path)
    dirs_to_make = ['logs', 'graphs', 'takeout']
    for dir_ in dirs_to_make:
        os.mkdir(dir_)

