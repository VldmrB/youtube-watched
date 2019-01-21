import os
from os.path import join
from flask import Flask, render_template, url_for
from flask import request, redirect, make_response, flash
from convert_takeout import get_all_records

app = Flask(__name__)
app.secret_key = '23lkjhv9z8y$!gffflsa1g4[p[p]'

flash_err = '<span style="color:Red;font-weight:bold">Error:</span>'
flash_note = '<span style="color:Blue;font-weight:bold">Note:</span>'
if os.name == 'nt':
    path_pattern = '[.a-zA-Z0-9_][^/?<>|*"]+'
else:
    path_pattern = '.+'


def strong(text):
    return f'<strong>{text}</strong>'


def get_project_dir_path_from_cookie():
    return request.cookies.get('project-dir')


@app.route('/')
def index():
    project_path = get_project_dir_path_from_cookie()
    if not project_path:
        return render_template('index.html')
    elif not os.path.exists(project_path):
        flash(f'{flash_err} could not find directory {strong(project_path)}')
        return render_template('index.html')

    api_key_path = join(project_path, 'api_key')
    if os.path.exists(api_key_path):
        with open(api_key_path, 'r') as api_file:
            api_key = True if api_file.read().strip() else None
    else:
        api_key = None

    return render_template('index.html', path=project_path, api_key=api_key,
                           path_pattern=path_pattern)


@app.route('/create_project_dir', methods=['POST'])
def create_project_dir():
    project_path = request.form['project-dir'].strip()
    if not project_path:
        flash(f'{flash_err} path cannot be empty ')
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


@app.route('/convert_takeout', methods=['POST'])
def populate_db():
    takeout_path = request.form.get('takeout-path')
    if not os.path.isdir(takeout_path):
        flash(f'{flash_err} {strong(takeout_path)} is not a directory or a '
              f'valid path')
        return redirect(url_for('index', failed=True))

    takeout_records = get_all_records(takeout_path, silent=True)
    if not takeout_records:
        flash(f'{flash_err} No watch-history files found.')
        return redirect(url_for('index', failed=True))

    project_path = get_project_dir_path_from_cookie()

    return redirect(url_for('index'))


if __name__ == '__main__':
    app.run()
    # from utils import logging_config
    # logging_config(r'C:\Users\Vladimir\Desktop\sql_fails.log')
    # write_to_sql.setup_db()

    # write_to_sql.create_all_tables(test_db_path)
    # drop_dynamic_tables(sqlite3.Connection(test_db_path))
    # create_all_tables(test_db_path)
    # insert_categories(test_db_path, takeout_path[:takeout_path.rfind('\\')])
    # insert_parent_topics(test_db_path)
    # insert_sub_topics(test_db_path)
    # write_to_sql.insert_videos()
