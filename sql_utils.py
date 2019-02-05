import sqlite3
import logging
from typing import Union

logger = logging.getLogger(__name__)


def sqlite_connection(db_path: str, **kwargs) -> sqlite3.Connection:
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


def generate_unconditional_update_query(table: str,
                                        columns: Union[list, tuple]):
    columns = ' = ?, '.join(columns).strip(',') + ' = ?'
    id_ = 'id_'  # PyCharm complains if id is literally set in the string itself - unresolved column name...
    return f'''UPDATE {table} SET {columns} WHERE {id_}= ?'''


def log_query_error(error, query_string: str, values=None):
    if not values:
        logger.error(f'{error}\n'
                     f'query = \'{query_string}\'')
        return
    logger.error(f'{error}\n'
                 f'query = \'{query_string}\'\n'
                 f'values = {values}')


def execute_query(conn: sqlite3.Connection,
                  query: str, values: Union[list, tuple] = None,
                  log_integrity_fail=True):
    """
    Executes the query with passed values (if any). If a SELECT query,
    returns the results.
    Passes potential errors to a logger. Logging for integrity errors,
    such as a foreign key constraint fail, can be disabled
    via log_integrity_fail param.
    """
    cur = conn.cursor()
    try:
        if values is not None:
            if isinstance(values, list):
                values = tuple(values)
            elif isinstance(values, tuple):
                pass
            else:
                raise ValueError('Expected str, tuple or list, got ' +
                                 values.__class__.__name__)
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
