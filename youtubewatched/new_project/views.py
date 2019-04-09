import os
from os.path import join

from flask import render_template, url_for, Blueprint
from flask import request, redirect, make_response, flash

from youtubewatched.utils.app import flash_note, flash_err, strong
from youtubewatched.utils.app import get_project_dir_path_from_cookie

from youtubewatched.manage_records.views import DBProcessState

setup_new_project = Blueprint('project', __name__)


@setup_new_project.route('/setup_project')
def setup_project():
    if DBProcessState.is_thread_alive():
        flash(f'{flash_err} Stop the current action before making a new '
              f'project')
        return redirect(url_for('records.index'))
    path = get_project_dir_path_from_cookie()
    return render_template('new_project.html', path=path)


@setup_new_project.route('/setup_project_form', methods=['POST'])
def setup_project_form():
    resp = make_response(redirect(url_for('records.index')))

    project_path = os.path.expanduser(request.form['project-dir'].strip())
    api_key = request.form.get('api-key')
    if api_key:
        api_key = api_key.strip()
    else:
        if os.path.exists(project_path):
            if os.path.exists(join(project_path, 'api_key')):
                resp.set_cookie('project-dir', project_path, max_age=31_536_000)
                return resp
            else:
                flash(f'{flash_err} No API key found in {strong(project_path)}')
                return redirect('setup_project')
        else:
            flash(f'{flash_err} {strong(project_path)} does not exist')
            return redirect('setup_project')

    try:
        if not os.path.exists(project_path):
            os.makedirs(project_path)
            flash(f'{flash_note} '
                  f'Created directory {os.path.abspath(project_path)}')

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
