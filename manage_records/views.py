import sqlite3
import time
from os.path import join
from threading import Thread
from time import sleep

from flask import Response, Blueprint, request

import write_to_sql
import youtube
from flask_utils import get_project_dir_path_from_cookie, flash_err
from sql_utils import sqlite_connection
from utils import load_file

record_management = Blueprint('records', __name__)


class ThreadControl:
    thread = None
    exit_thread = False
    live_thread_warning = 'Wait for the current operation to finish'
    
    def is_thread_alive(self):
        return self.thread and self.thread.is_alive()

    def exit_thread_and_clean_up(self):
        if self.exit_thread:
            # self.exit_thread = False
            print('Stopped the thread!')
            progress.clear()
            progress.append('Error: Process stopped')
            return True


DBProcessState = ThreadControl()
progress = []


@record_management.route('/cancel_db_process', methods=['POST'])
def cancel_db_process():
    if DBProcessState.thread and DBProcessState.thread.is_alive():
        DBProcessState.exit_thread = True
        while True:
            if DBProcessState.is_thread_alive():
                sleep(0.5)
            else:
                DBProcessState.exit_thread = False
                break
    return 'Process stopped'


def db_stream_event():
    while True:
        if progress:
            cur_val = str(progress.pop(0))
            yield 'data: ' + cur_val + '\n'*2
            if 'records_processed' in cur_val or 'Error' in cur_val:
                break
        else:
            sleep(0.05)


@record_management.route('/db_progress_stream')
def db_progress_stream():
    return Response(db_stream_event(), mimetype="text/event-stream")


@record_management.route('/convert_takeout', methods=['POST'])
def populate_db_form():
    
    if DBProcessState.is_thread_alive():
        return DBProcessState.live_thread_warning

    takeout_path = request.form['takeout-dir']

    project_path = get_project_dir_path_from_cookie()
    DBProcessState.thread = Thread(target=populate_db,
                                   args=(takeout_path, project_path))
    DBProcessState.thread.start()

    return ''


def populate_db(takeout_path: str, project_path: str):
    from convert_takeout import get_all_records

    if DBProcessState.exit_thread_and_clean_up():
        return

    progress.clear()
    progress.append('Locating and processing watch-history.html files...')
    try:
        records = get_all_records(takeout_path)
        progress.append('Inserting records...')
    except FileNotFoundError:
        progress.append(f'{flash_err} Invalid/non-existent path for '
                        f'watch-history.html files')
        raise

    if DBProcessState.exit_thread_and_clean_up():
        return

    if records is False:
        progress.append(f'{flash_err} No watch-history files found in '
                        f'"{takeout_path}"')
        raise ValueError('No watch-history files found')
    db_path = join(project_path, 'yt.sqlite')
    conn = sqlite_connection(db_path)
    try:
        api_auth = youtube.get_api_auth(
            load_file(join(project_path, 'api_key')).strip())
        write_to_sql.setup_tables(conn, api_auth)
        tm_start = time.time()
        for records_processed in write_to_sql.insert_videos(
                conn, records, api_auth):
            if DBProcessState.exit_thread_and_clean_up():
                return
            if isinstance(records_processed, int):
                progress.append(str(records_processed))
            else:
                progress.append(records_processed)
        print(time.time() - tm_start, 'seconds!')
        conn.close()
    except youtube.ApiKeyError:
        progress.append(f'{flash_err} Missing or invalid API key')
        raise
    except (sqlite3.OperationalError, sqlite3.DatabaseError) as e:
        progress.append(f'{flash_err} Fatal database error - {e!r}')
        raise
    except FileNotFoundError:
        progress.append(f'{flash_err} Invalid database path')
        raise

    conn.close()


@record_management.route('/update_records')
def update_db_form():
    if DBProcessState.is_thread_alive():
        return DBProcessState.live_thread_warning

    project_path = get_project_dir_path_from_cookie()
    
    DBProcessState.thread = Thread(target=update_db, args=(project_path,))
    DBProcessState.thread.start()

    return ''


def update_db(project_path: str):
    import sqlite3
    import write_to_sql
    import youtube
    import time
    from utils import load_file

    progress.clear()
    progress.append('Starting updating...')
    db_path = join(project_path, 'yt.sqlite')
    decl_types = sqlite3.PARSE_DECLTYPES
    decl_colnames = sqlite3.PARSE_COLNAMES
    # declarations are for the timestamps (maybe for more as well, later)
    conn = sqlite_connection(db_path,
                             detect_types=decl_types | decl_colnames)
    try:
        api_auth = youtube.get_api_auth(
            load_file(join(project_path, 'api_key')).strip())
        tm_start = time.time()
        progress.append('Updating...')
        for records_processed in write_to_sql.update_videos(conn, api_auth):
            if isinstance(records_processed, int):
                progress.append(str(records_processed))
            else:
                progress.append(records_processed)
        print(time.time() - tm_start, 'seconds!')
    except youtube.ApiKeyError:
        progress.append(f'{flash_err} Missing or invalid API key')
        raise
    except (sqlite3.OperationalError, sqlite3.DatabaseError) as e:
        progress.append(f'{flash_err} Fatal database error - {e!r}')
        raise
    except FileNotFoundError:
        progress.append(f'{flash_err} Invalid database path')
        raise

    conn.close()
