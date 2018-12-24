import os
import argparse
import logging
from convert_takeout import get_all_records
import write_to_sql
from utils import logging_config


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

    tk_path = r'G:\pyton\youtube_watched_data\takeout_data'
    test_db_path = r'G:\pyton\db_test.sqlite'

    write_to_sql.create_all_tables(test_db_path)
