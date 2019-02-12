import os
import sqlite3
import json
from os.path import join
from flask import Flask, render_template, url_for
from flask import request, redirect, make_response, flash
from sql_utils import sqlite_connection
from flask_utils import get_project_dir_path_from_cookie, strong
from flask_utils import flash_note, flash_err
from utils import logging_config

from manage_records.views import record_management

app = Flask(__name__)
app.secret_key = '23lkjhv9z8y$!gffflsa1g4[p[p]'

app.register_blueprint(record_management)

configure_logging = False


@app.route('/')
def index():
    project_path = get_project_dir_path_from_cookie()
    if not project_path:
        return redirect(url_for('setup_project'))
    elif not os.path.exists(project_path):
        flash(f'{flash_err} could not find directory {strong(project_path)}')
        return redirect(url_for('setup_project'))

    global configure_logging
    if not configure_logging:
        logging_config(join(project_path, 'events.log'))
        configure_logging = True

    db_path = join(project_path, 'yt.sqlite')
    db = None
    if os.path.exists(db_path):
        conn = sqlite_connection(db_path)
        cur = conn.cursor()
        try:
            cur.execute("SELECT 'id' FROM videos")
            total_records = len(cur.fetchall())
            if total_records:
                db = {}
                records = f'{total_records} records'
                db['records'] = records
                cur.execute('SELECT id FROM channels')
                total_channels = len(cur.fetchall())
                channels = f'{total_channels} channels'
                db['channels'] = channels
                cur.execute('SELECT id FROM tags')
                total_tags = cur.fetchall()
                if total_tags:  # a tiny chance of Takeout with 1-5 records 
                    # with no tags...
                    tags = f'{total_tags} unique tags'
                    db['tags'] = tags
                db = json.dumps(db)
            cur.close()
        except sqlite3.OperationalError:
            pass
        conn.close()
    if not request.cookies.get('description-seen'):
        resp = make_response(render_template('index.html', path=project_path,
                                             description=True, db=db))
        resp.set_cookie('description-seen', 'True', max_age=31_536_000)
        return resp
    return render_template('index.html', path=project_path, db=db)


@app.route('/setup_project')
def setup_project():
    path = get_project_dir_path_from_cookie()
    return render_template('new_project.html', path=path)


@app.route('/setup_project_form', methods=['POST'])
def setup_project_form():
    resp = make_response(redirect(url_for('index')))

    project_path = request.form['project-dir'].strip()
    api_key = request.form.get('api-key')
    if api_key:
        api_key = api_key.strip()
    else:
        if os.path.exists(project_path) and os.path.exists(
                join(project_path, 'api_key')):
            resp.set_cookie('project-dir', project_path, max_age=31_536_000)
            return resp
        else:
            flash(f'{flash_err} No valid project found at '
                  f'{strong(project_path)}')
            return redirect('setup_project')

    try:
        if not os.path.exists(project_path):
            os.makedirs(project_path)
            flash(f'{flash_note} '
                  f'Created directory {os.path.abspath(project_path)}')
        dirs_to_make = ['logs', 'graphs']
        for dir_ in dirs_to_make:
            os.mkdir(join(project_path, dir_))

        with open(join(project_path, 'api_key'), 'w') as api_file:
            api_file.write(api_key)
    except FileNotFoundError:
        raise
    except FileExistsError:
        pass
    except OSError:
        flash(f'{flash_err} {strong(project_path)} is not a valid path')
        return redirect('setup_project')
    resp.set_cookie('project-dir', project_path, max_age=31_536_000)

    return resp


if __name__ == '__main__':
    app.run()
    pass
