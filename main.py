import os
import shutil
import json
# import logging
# from utils import logging_config
# import write_to_sql
from utils import load_file
from os.path import join
from flask import Flask, render_template, url_for
from flask import request, redirect, make_response, flash

PROFILES_JSON = 'profiles.json'
app = Flask(__name__)
app.secret_key = '23lkjhv9z8y$!gffflsa1g4[p[p]'

flash_err = '<span style="color:Red;font-weight:bold">Error:</span>'
flash_note = '<span style="color:Blue;font-weight:bold">Note:</span>'


def strong(text):
    return f'<strong>{text}</strong>'


@app.route('/')
def index():
    path = request.cookies.get('project-dir')
    if not path:
        return render_template('index.html')
    elif not os.path.exists(path):
        flash(f'{flash_err} could not find directory {strong(path)}')
        return render_template('index.html')

    return render_template('index.html', path=path)


@app.route('/create_project_dir', methods=['POST'])
def setup_project_dir():
    if request.method == 'POST':
        path = request.form['project-dir']
        try:
            os.makedirs(path, exist_ok=True)
        except FileNotFoundError:
            raise
        except OSError:
            flash(f'{flash_err} invalid path.')
            return redirect(url_for('index'))
        resp = make_response(redirect(url_for('index')))
        if path:
            dirs_to_make = ['logs', 'graphs']
            try:
                if not os.path.exists(path):
                    os.mkdir(path)
                for dir_ in dirs_to_make:
                    os.mkdir(join(path, dir_))
            except FileExistsError:
                pass
            except OSError:
                flash(f'{flash_err} {strong(path)} is not a valid profile name')
            resp.set_cookie('project-dir', path, max_age=31_536_000)
            return resp
    return redirect(url_for('index'))


if __name__ == '__main__':
    test_db_path = r'G:\pyton\youtube_watched_data\test2'
    # os.chdir(test_db_path)
    # log_path = r'logs\sql_fails.log'
    # logging_config(log_path)
    # logger = logging.getLogger(__name__)
    # write_to_sql.setup_db()

    # write_to_sql.create_all_tables(test_db_path)
    # drop_dynamic_tables(sqlite3.Connection(test_db_path))
    # create_all_tables(test_db_path)
    # insert_categories(test_db_path, takeout_path[:takeout_path.rfind('\\')])
    # insert_parent_topics(test_db_path)
    # insert_sub_topics(test_db_path)
    #
    # write_to_sql.insert_videos()

    app.run()
