import os
import sqlite3
import argparse
import logging
# from pprint import pprint
from convert_takeout import get_all_records
import youtube
from utils import logging_config
from confidential import DEVELOPER_KEY


def insert_videos_into_sql(path: str = None):
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
    from confidential import DEVELOPER_KEY
    if not DEVELOPER_KEY:  # this and the line above must be removed once this
        # is a library
        if os.path.exists('api_key'):
            with open('api_key', 'r') as file:
                DEVELOPER_KEY = file.read().strip()
        else:
            raise SystemExit(
                'An API key must be assigned to DEVELOPER_KEY'
                'to retrieve video info.')
    rows_passed = 0
    sql_fails = 0
    decl_types = sqlite3.PARSE_DECLTYPES
    decl_colnames = sqlite3.PARSE_COLNAMES
    # conn = utils.sqlite_connection(path,
    #                                detect_types=decl_types | decl_colnames)
    # cur = conn.cursor()
    # cur.execute("""SELECT id FROM videos;""")
    # video_ids = [row[0] for row in cur.fetchall()]
    # cur.execute("""SELECT id FROM channels;""")
    # channels = [row[0] for row in cur.fetchall()]
    # cur.execute("""SELECT * FROM tags;""")
    # existing_tags = {v: k for k, v in cur.fetchall()}
    # cur.close()
    # logger.info(f'\nStarting records\' insertion...\n' + '-'*100)
    records = get_all_records(path)
    if 'unknown' in records:
        unknown = records.pop('unknown')


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


def setup_data_dir(path: str):
    """
    For usage with CLI. Sets up the initial data dir, which will then be used
    for storing all the necessary data and generated graphs.
    """
    os.makedirs(path, exist_ok=True)
    os.chdir(path)
    dirs_to_make = ['logs', 'graphs']
    for dir_ in dirs_to_make:
        try:
            os.mkdir(dir_)
        except FileExistsError:
            pass


def argparse_func():
    engine = argparse.ArgumentParser()
    parsers = engine.add_subparsers(title='Statistics',
                                    help='Generates basic stats from data in '
                                         'located watch-history.html file(s)')

    stat_p = parsers.add_parser('stats')
    stat_p.set_defaults(func=get_all_records)

    stat_p.add_argument('--dir',
                        help='directory with the watch-history.html file(s)')
    stat_p.add_argument('-i', '--in-place', default=False, dest='write_changes',
                        help='Trim unnecessary HTML from the found files for '
                             'faster processing next time (no data is lost)')

    if __name__ == '__main__':
        pass
        # args = engine.parse_args()
        # args.func(args.dir, args.write_changes)


if __name__ == '__main__':
    log_path = r'C:\Users\Vladimir\Desktop\fails.log'
    logging_config(log_path)
    logger = logging.getLogger(__name__)
    # takeout_path = r'G:\pyton\youtube_watched_data\takeout_data'
    # insert_videos_into_sql(takeout_path)
    youtube.get_video_info('AgC4DM1EZ5A', youtube.get_api_auth(DEVELOPER_KEY))
