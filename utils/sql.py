import logging
import os
import sqlite3
from os.path import join

from utils.app import get_project_dir_path_from_cookie
from config import DB_NAME
from typing import Union

logger = logging.getLogger(__name__)


def sqlite_connection(db_path: str, types=False,
                      **kwargs) -> sqlite3.Connection:
    if types:
        types = sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
        conn = sqlite3.connect(db_path, detect_types=types, **kwargs)
    else:
        conn = sqlite3.connect(db_path, **kwargs)
    cur = conn.cursor()
    cur.execute('PRAGMA foreign_keys=ON;')
    conn.commit()
    cur.close()
    return conn


def generate_insert_query(table: str,
                          columns: Union[list, tuple],
                          on_conflict_ignore=False)-> str:
    """
    Constructs a basic insert query.
    """

    val_amount = len(columns)
    values_placeholders = '(' + ('?, ' * val_amount).strip(' ,') + ')'
    columns = '(' + ', '.join(columns) + ')'

    query = f' INTO {table} {columns} VALUES {values_placeholders}'
    if on_conflict_ignore:
        query = f'INSERT OR IGNORE' + query
    else:
        query = 'INSERT' + query
    return query


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


def generate_unconditional_update_query(columns: Union[list, tuple]):
    columns = ' = ?, '.join(columns).strip(',') + ' = ?'
    return f'''UPDATE videos SET {columns} WHERE id = ?'''


def log_query_error(error, query_string: str, values=None):
    if not values:
        logger.error(f'{error}\n'
                     f'query = \'{query_string}\'')
        return
    logger.error(f'{error}\n'
                 f'query = \'{query_string}\'\n'
                 f'values = {values}')


def execute_query(conn: sqlite3.Connection,
                  query: str, values: tuple = None,
                  log_integrity_fail=True):
    """
    Executes the query with passed values (if any). If a SELECT query,
    returns the results.
    Passes potential errors to a logger. Logging for integrity errors,
    such as a foreign key constraint fail, can be disabled
    via the log_integrity_fail param.
    """
    cur = conn.cursor()
    try:
        if values is not None:
            cur.execute(query, values)
        else:
            cur.execute(query)
        if query.lower().startswith('select'):
            return cur.fetchall()
        return True
    except sqlite3.IntegrityError as e:
        if not log_integrity_fail:
            return False
        if values:
            values = f'{list(values)}'
            log_query_error(e, query, values)
        else:
            log_query_error(e, query)
        return False
    except sqlite3.Error as e:
        if values:
            values = f'{list(values)}'
            logger.error('FATAL ERROR:')
            log_query_error(e, query, values)
        else:
            log_query_error(e, query)
        raise
    finally:
        cur.close()
