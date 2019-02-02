import os
from utils import logging_config
from os.path import join
from time import sleep
from flask import Flask, Response, render_template, url_for
from flask import request, redirect, make_response, flash
from sql_utils import sqlite_connection
from convert_takeout import get_all_records

app = Flask(__name__)
app.secret_key = '23lkjhv9z8y$!gffflsa1g4[p[p]'

flash_err = '<span style="color:Red;font-weight:bold">Error:</span>'
flash_note = '<span style="color:Blue;font-weight:bold">Note:</span>'
if os.name == 'nt':
    path_pattern = '[.a-zA-Z0-9_][^/?<>|*"]+'
else:
    path_pattern = '.+'

configure_logging = False
insert_videos_thread = None
progress = []


def strong(text):
    return f'<strong>{text}</strong>'


def get_project_dir_path_from_cookie():
    return request.cookies.get('project-dir')


@app.route('/')
def index():
    project_path = get_project_dir_path_from_cookie()
    if not project_path:
        return render_template('index.html', path_pattern=path_pattern)
    elif not os.path.exists(project_path):
        flash(f'{flash_err} could not find directory {strong(project_path)}')
        return render_template('index.html', path_pattern=path_pattern)
    global configure_logging
    if not configure_logging:
        configure_logging = True
        logging_config(join(project_path, 'events.log'))
    api_key_path = join(project_path, 'api_key')
    if os.path.exists(api_key_path):
        with open(api_key_path, 'r') as api_file:
            api_key = True if api_file.read().strip() else None
    else:
        api_key = None

    if insert_videos_thread and insert_videos_thread.is_alive():
        thread = 'live'
    else:
        thread = None
    if os.path.exists(join(project_path, 'yt.sqlite')):
        db = True
    else:
        db = None

    return render_template('index.html', path=project_path, api_key=api_key,
                           path_pattern=path_pattern, thread=thread,
                           db=db)


@app.route('/create_project_dir', methods=['POST'])
def create_project_dir():
    project_path = request.form['project-dir'].strip()
    if not project_path:
        flash(f'{flash_err} path cannot be empty')
        return redirect(url_for('index'))
    resp = make_response(redirect(url_for('index')))

    try:
        if not os.path.exists(project_path):
            os.makedirs(project_path)
            flash(f'{flash_note} '
                  f'Created directory {os.path.abspath(project_path)}')
        dirs_to_make = ['logs', 'graphs']
        for dir_ in dirs_to_make:
            os.mkdir(join(project_path, dir_))
    except FileNotFoundError:
        raise
    except FileExistsError:
        pass
    except OSError:
        flash(f'{flash_err} {strong(project_path)} is not a valid path')
        return redirect(url_for('index'))
    resp.set_cookie('project-dir', project_path, max_age=31_536_000)
    return resp


@app.route('/setup_api_key', methods=['POST'])
def setup_api_key():
    api_key = request.form['api-key']
    project_path = get_project_dir_path_from_cookie()
    with open(join(project_path, 'api_key'), 'w') as api_file:
        api_file.write(api_key)

    return redirect(url_for('index'))


def db_stream_event():
    while True:
        if progress:
            cur_val = str(progress.pop(0))
            yield 'data: ' + cur_val + '\n'*2
            if 'records_processed' in cur_val:
                break
        else:
            sleep(0.05)


@app.route('/get_progress')
def db_progress_stream():
    return Response(db_stream_event(), mimetype="text/event-stream")


@app.route('/convert_takeout', methods=['POST'])
def populate_db_form():
    from threading import Thread
    takeout_path = request.form.get('takeout-path')

    project_path = get_project_dir_path_from_cookie()
    global insert_videos_thread
    insert_videos_thread = Thread(target=populate_db,
                                  args=(takeout_path, project_path))
    insert_videos_thread.start()

    return ''


def populate_db(takeout_path: str, project_path: str):
    import sqlite3
    import write_to_sql
    import youtube
    import time
    from utils import load_file
    progress.append('Locating and processing watch-history.html files...')
    try:
        records = get_all_records(takeout_path)
        progress.append('Inserting records...')
    except FileNotFoundError:
        progress.append(f'{flash_err} Invalid/non-existent path for '
                        f'watch-history.html files')
        raise
    if records is False:
        progress.append(f'{flash_err} No watch-history files found in '
                        f'"{takeout_path}"')
        raise ValueError('No watch-history files found')
    try:
        api_auth = youtube.get_api_auth(
            load_file(join(project_path, 'api_key')).strip())
        db_path = join(project_path, 'yt.sqlite')
        decl_types = sqlite3.PARSE_DECLTYPES
        decl_colnames = sqlite3.PARSE_COLNAMES
        # declarations are for the timestamps (maybe for more as well, later)
        conn = sqlite_connection(db_path,
                                 detect_types=decl_types | decl_colnames)
        write_to_sql.setup_tables(conn, api_auth)
        tm_start = time.time()
        for records_processed in write_to_sql.insert_videos(
                conn, records, api_auth):
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
    finally:
        # in case the page is refreshed before this finishes running, to
        # prevent old progress messages from potentially showing up
        # in the next run
        sleep(1)
        progress.clear()
        pass


if __name__ == '__main__':
    app.run()
    # from utils import logging_config
    # logging_config(r'C:\Users\Vladimir\Desktop\sql_fails.log')
