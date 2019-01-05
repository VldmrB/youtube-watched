import os
import json
# import logging
# from utils import logging_config
# import write_to_sql
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
    path = request.cookies.get('data_dir')
    if not path:
        return render_template('index.html', path=path)
    if not os.path.exists(path):
        flash(f'{flash_err} could not find directory {strong(path)}')
        return render_template('index.html')
    if not os.path.exists(join(path, PROFILES_JSON)):
        with open(join(path, PROFILES_JSON), 'w') as file:
            json.dump({'profiles': [], 'current_profile': ''}, file)
        return render_template('index.html', path=path)
    else:
        with open(join(path, PROFILES_JSON), 'r') as file:
            profiles_dict = json.load(file)
            cur_profile = profiles_dict.get('current_profile', None)
            new_profiles_list = []
            for profile in profiles_dict['profiles']:
                if os.path.exists(join(path, profile)):
                    new_profiles_list.append(profile)
            if profiles_dict['profiles'] != new_profiles_list:
                profiles_dict['profiles'] = new_profiles_list
                with open(join(path, PROFILES_JSON), 'w') as profiles_file:
                    json.dump(profiles_dict, profiles_file)
            if cur_profile and cur_profile not in profiles_dict['profiles']:
                flash(f'{flash_err} could not find profile '
                      f'{strong(cur_profile)}')
                cur_profile = None

        return render_template('index.html', path=path,
                               profiles=profiles_dict['profiles'],
                               cur_profile=cur_profile)


@app.route('/create_data_dir', methods=['POST'])
def setup_data_dir():
    if request.method == 'POST':
        path = request.form['data_dir']
        try:
            os.makedirs(path, exist_ok=True)
        except FileNotFoundError:
            raise
        except OSError:
            flash(f'{flash_err} invalid path.')
            return redirect(url_for('index'))
        resp = make_response(redirect(url_for('index')))
        if path:
            resp.set_cookie('data_dir', path, max_age=31_536_000)
            return resp
    return redirect(url_for('index'))


@app.route('/setup_profile_dir', methods=['POST'])
def setup_profile_dir():
    if request.method == 'POST':
        data_dir_path = request.cookies.get('data_dir')
        path = request.form['profile']
        if os.sep in path:
            path = path[path.rfind(os.sep)+1:]
        with open(join(data_dir_path, PROFILES_JSON), 'r') as file:
            profiles_dict = json.load(file)
        if path in profiles_dict['profiles']:
            flash(f'{flash_note} switched to profile {strong(path)}')
            profiles_dict['current_profile'] = path
            with open(join(data_dir_path, PROFILES_JSON), 'w') as file:
                json.dump(profiles_dict, file)
        else:
            dirs_to_make = ['logs', 'graphs']
            full_path = join(data_dir_path, path)
            try:
                # necessary in case user mistakenly deletes/removes a profile,
                # then manually adds it back. Otherwise, will throw
                # FileExistsError out before registering the profile in
                # profiles.json (thus causing the profile to never show up in
                # the app) and do that ad infinitum
                if not os.path.exists(full_path):
                    os.mkdir(full_path)
                profiles_dict['profiles'].append(path)
                profiles_dict['current_profile'] = path
                with open(join(data_dir_path, PROFILES_JSON), 'w') as file:
                    json.dump(profiles_dict, file)
                for dir_ in dirs_to_make:
                    os.mkdir(join(full_path, dir_))
            except FileExistsError:
                pass
            except OSError:
                flash(f'{flash_err} {strong(path)} is not a valid profile name')
    return redirect(url_for('index'))


@app.route('/profiles')
def profiles():
    data_dir = request.cookies.get('data_dir')
    try:
        with open(join(data_dir, PROFILES_JSON), 'r') as file:
            results = json.load(file)
            if results:
                profiles_list = results['profiles']
            else:
                profiles_list = None
    except FileNotFoundError:
        profiles_list = None

    return render_template('profile_list.html', profiles=profiles_list)


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
