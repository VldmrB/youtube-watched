import os
from os.path import join

from flask import render_template, url_for, Blueprint
from flask import request, redirect, make_response, flash

from utils.app import flash_note, flash_err, strong
from utils.app import get_project_dir_path_from_cookie

setup_new_project = Blueprint('project', __name__)


@setup_new_project.route('/setup_project')
def setup_project():
    path = get_project_dir_path_from_cookie()
    return render_template('new_project.html', path=path)


@setup_new_project.route('/setup_project_form', methods=['POST'])
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
