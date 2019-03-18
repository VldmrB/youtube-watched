from os.path import join
from flask import request

from config import DB_NAME

flash_err = '<span style="color:Red;font-weight:bold;">Error:</span>'
flash_note = '<span style="color:Blue;font-weight:bold">Note:</span>'


def strong(text):
    return f'<strong>{text}</strong>'


def get_project_dir_path_from_cookie():
    return request.cookies.get('project-dir')


def get_db_path():
    return join(get_project_dir_path_from_cookie(), DB_NAME)
