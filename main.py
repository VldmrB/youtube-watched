import os
import argparse
from convert_takeout import get_all_records
# import logging
# from utils import logging_config
# import write_to_sql

from flask import Flask, render_template, url_for
from flask import request, redirect, make_response, flash

UPLOAD_FOLDER = 'files'
ALLOWED_EXTENSIONS = {'txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'}

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.secret_key = '23lkjhv9z8y$!gffflsa1g4[p[p]'

flash_err = '<strong>Error:</strong>'


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/')
def index():
    path = request.cookies.get('data_dir')
    if not path:
        return render_template('index.html', path=path)
    if not os.path.exists(path):
        flash(f'{flash_err} could not find directory {path}')
        resp = make_response(render_template('index.html', path=None))
        # resp.set_cookie('data_dir', max_age=0)
        print('What!')
        return resp
    return render_template('index.html', path=path)


@app.route('/create_data_dir', methods=['GET', 'POST'])
def setup_data_dir():
    if request.method == 'POST':
        path = request.form['text']
        if not path:
            return redirect(url_for('index'))
        try:
            os.makedirs(path, exist_ok=True)
            os.chdir(path)
            dirs_to_make = ['logs', 'graphs']
            for dir_ in dirs_to_make:
                try:
                    os.mkdir(dir_)
                except FileExistsError:
                    pass
            resp = make_response(redirect(url_for('index')))
            if path:
                resp.set_cookie('data_dir', path, max_age=31_536_000)
            return resp

        except FileNotFoundError:
            raise
        except OSError:
            flash(f'{flash_err} invalid path.')
            return redirect(url_for('index'))

    return redirect(url_for('index'))


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
    test_db_path = r'G:\pyton\youtube_watched_data\test2'
    # os.chdir(test_db_path)
    # setup_data_dir(test_db_path)
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
