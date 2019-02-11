from flask import request

flash_err = '<span style="color:Red;font-weight:bold;">Error:</span>'
flash_note = '<span style="color:Blue;font-weight:bold">Note:</span>'


def strong(text):
    return f'<strong>{text}</strong>'


def get_project_dir_path_from_cookie():
    return request.cookies.get('project-dir')
