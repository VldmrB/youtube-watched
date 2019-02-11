from flask import request


def strong(text):
    return f'<strong>{text}</strong>'


def get_project_dir_path_from_cookie():
    return request.cookies.get('project-dir')
