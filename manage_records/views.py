import json
import os
import sqlite3
import time
from os.path import join
from threading import Thread
from time import sleep

from flask import (Response, Blueprint, request, redirect, make_response,
                   render_template, url_for, flash)

import write_to_sql
import youtube
from utils.app import (get_project_dir_path_from_cookie, flash_err, strong)
from utils.sql import sqlite_connection, db_has_records, execute_query
from utils.gen import load_file

record_management = Blueprint('records', __name__)


class ThreadControl:
    thread = None
    exit_thread_flag = False
    live_thread_warning = 'Wait for the current operation to finish'

    active_event_stream = None
    stage = None
    percent = '0.0'

    def is_thread_alive(self):
        return self.thread and self.thread.is_alive()

    def exit_thread_check(self):
        if self.exit_thread_flag:
            DBProcessState.stage = None
            print('Stopped the DB update thread')
            return True


DBProcessState = ThreadControl()
progress = []


def add_sse_event(data: str = '', event: str = '', id_: str = ''):
    progress.append(f'data: {data}\n'
                    f'event: {event}\n'
                    f'id: {id_}\n\n')
    if event in ['errors', 'stats']:
        DBProcessState.stage = None


@record_management.route('/')
def index():
    project_path = get_project_dir_path_from_cookie()
    if not project_path:
        return redirect(url_for('setup_project'))
    elif not os.path.exists(project_path):
        flash(f'{flash_err} could not find directory {strong(project_path)}')
        return redirect(url_for('setup_project'))

    if DBProcessState.active_event_stream is None:
        DBProcessState.active_event_stream = True
    else:
        # event_stream() will set this back to True after disengaging
        DBProcessState.active_event_stream = False

    db = db_has_records()
    if not request.cookies.get('description-seen'):
        resp = make_response(render_template('index.html', path=project_path,
                                             description=True, db=db))
        resp.set_cookie('description-seen', 'True', max_age=31_536_000)
        return resp
    return render_template('index.html', path=project_path, db=db)


@record_management.route('/process_status')
def process_status():
    if not DBProcessState.stage:
        return json.dumps({'stage': 'Quiet'})
    else:
        return json.dumps({'stage': DBProcessState.stage,
                           'percent': DBProcessState.percent})


@record_management.route('/cancel_db_process', methods=['POST'])
def cancel_db_process():
    DBProcessState.stage = None
    DBProcessState.percent = '0.0'
    if DBProcessState.thread and DBProcessState.thread.is_alive():
        DBProcessState.exit_thread_flag = True
        while True:
            if DBProcessState.is_thread_alive():
                sleep(0.5)
            else:
                DBProcessState.exit_thread_flag = False
                break
    return 'Process stopped'


def event_stream():
    while True:
        if progress:
            yield progress.pop(0)
        else:
            if DBProcessState.active_event_stream:
                sleep(0.05)
            else:
                break

    # allow SSE for potential subsequent Takeout processes
    DBProcessState.active_event_stream = True
    progress.clear()


@record_management.route('/db_progress_stream')
def db_progress_stream():
    return Response(event_stream(), mimetype="text/event-stream")


@record_management.route('/convert_takeout', methods=['POST'])
def populate_db_form():
    
    if DBProcessState.is_thread_alive():
        return DBProcessState.live_thread_warning

    takeout_path = request.form['takeout-dir'].strip()

    project_path = get_project_dir_path_from_cookie()
    DBProcessState.thread = Thread(target=populate_db,
                                   args=(takeout_path, project_path))
    DBProcessState.thread.start()

    return ''


def populate_db(takeout_path: str, project_path: str):
    from convert_takeout import get_all_records

    if DBProcessState.exit_thread_check():
        return

    progress.clear()

    DBProcessState.percent = '0.0'
    DBProcessState.stage = 'Locating and processing watch-history.html files...'
    add_sse_event(DBProcessState.stage, 'stage')
    try:
        records = get_all_records(takeout_path)
    except FileNotFoundError:
        add_sse_event(f'Invalid/non-existent path for watch-history.html files',
                      'errors')
        raise

    if DBProcessState.exit_thread_check():
        return

    if not records:
        add_sse_event(f'No Takeout directories found in "{takeout_path}"',
                      'errors')
        raise ValueError('No watch-history files found')
    db_path = join(project_path, 'yt.sqlite')
    conn = sqlite_connection(db_path)
    results = {'updated': 0, 'failed_api_requests': 0}
    try:
        api_auth = youtube.get_api_auth(
            load_file(join(project_path, 'api_key')).strip())
        write_to_sql.setup_tables(conn, api_auth)
        records_at_start = results['records_in_db'] = execute_query(
            conn, 'SELECT count(*) from videos')[0][0]

        DBProcessState.stage = 'Inserting records...'
        add_sse_event(DBProcessState.stage, 'stage')

        tm_start = time.time()
        for record in write_to_sql.insert_videos(
                conn, records, api_auth):
            DBProcessState.percent = str(record[0])
            add_sse_event(DBProcessState.percent)
            results['updated'] = record[1]
            results['failed_api_requests'] = record[2]

            if DBProcessState.exit_thread_check():
                break

        results['records_in_db'] = execute_query(
            conn, 'SELECT count(*) from videos')[0][0]
        results['inserted'] = results['records_in_db'] - records_at_start
        add_sse_event(json.dumps(results), 'stats')
        print(time.time() - tm_start, 'seconds!')
        conn.close()
    except youtube.ApiKeyError:
        add_sse_event(f'Missing or invalid API key', 'errors')
        raise
    except (sqlite3.OperationalError, sqlite3.DatabaseError) as e:
        add_sse_event(f'Fatal database error - {e!r}', 'errors')
        raise
    except FileNotFoundError:
        add_sse_event(f'Invalid database path', 'errors')
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
    from utils.gen import load_file

    progress.clear()
    DBProcessState.percent = '0.0'
    DBProcessState.stage = 'Starting updating...'
    add_sse_event(DBProcessState.stage, 'stage')
    db_path = join(project_path, 'yt.sqlite')
    conn = sqlite_connection(db_path)
    results = {'updated': 0,
               'failed_api_requests': 0,
               'records_in_db': execute_query(
                   conn,
                   'SELECT count(*) from videos')[0][0]}
    try:
        api_auth = youtube.get_api_auth(
            load_file(join(project_path, 'api_key')).strip())
        tm_start = time.time()
        DBProcessState.stage = 'Updating...'
        add_sse_event(DBProcessState.stage, 'stage')
        for record in write_to_sql.update_videos(conn, api_auth, 86400):
            if DBProcessState.exit_thread_check():
                break
            DBProcessState.percent = str(record[0])
            add_sse_event(DBProcessState.percent)
            results['updated'] = record[1]
            results['failed_api_requests'] = record[2]
        print(time.time() - tm_start, 'seconds!')
        add_sse_event(json.dumps(results), 'stats')
    except youtube.ApiKeyError:
        add_sse_event(f'{flash_err} Missing or invalid API key', 'errors')
        raise
    except (sqlite3.OperationalError, sqlite3.DatabaseError) as e:
        add_sse_event(f'{flash_err} Fatal database error - {e!r}', 'errors')
        raise
    except FileNotFoundError:
        add_sse_event(f'{flash_err} Invalid database path', 'errors')
        raise

    conn.close()
