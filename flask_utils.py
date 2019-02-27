import os
import sqlite3
from os.path import join

from flask import request

from config import DB_NAME
from sql_utils import sqlite_connection

flash_err = '<span style="color:Red;font-weight:bold;">Error:</span>'
flash_note = '<span style="color:Blue;font-weight:bold">Note:</span>'


def strong(text):
    return f'<strong>{text}</strong>'


def get_project_dir_path_from_cookie():
    return request.cookies.get('project-dir')


def db_has_records():
    db_path = join(get_project_dir_path_from_cookie(), DB_NAME)
    if os.path.exists(db_path):
        conn = sqlite_connection(db_path)
        cur = conn.cursor()
        try:
            cur.execute("SELECT 'id' FROM videos")
            total_records = len(cur.fetchall())
            if total_records:
                return True
            cur.close()
        except sqlite3.OperationalError:
            return
        finally:
            conn.close()


