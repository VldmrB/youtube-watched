import sys
import sqlite3
import logging
from logging import handlers
from typing import Union


def is_video_url(candidate_str: str):
    return True if 'youtube.com/watch?' in candidate_str else False


def get_video_id(url):
    video_id = url[url.find('=') + 1:]
    id_end = video_id.find('&t=')
    if id_end > 0:
        video_id = video_id[:id_end]

    return video_id


def convert_duration(duration_iso8601: str):
    duration = duration_iso8601.split('T')
    duration = {'P': duration[0][1:], 'T': duration[1]}
    int_value = 0
    for key, value in duration.items():
        new_value = ''
        if not value:
            continue
        for element in value:
            if element.isnumeric():
                new_value += element
            else:
                new_value += element + ' '
        split_vals = new_value.strip().split(' ')
        for val in split_vals:
            if val[-1] == 'Y':
                int_value += int(val[:-1]) * 31_536_000
            elif val[-1] == 'M':
                if key == 'P':
                    int_value += int(val[:-1]) * 2_592_000
                else:
                    int_value += int(val[:-1]) * 60
            elif val[-1] == 'W':
                int_value += int(val[:-1]) * 604800
            elif val[-1] == 'D':
                int_value += int(val[:-1]) * 86400
            elif val[-1] == 'H':
                int_value += int(val[:-1]) * 3600
            elif val[-1] == 'S':
                int_value += int(val[:-1])

    return int_value


def get_final_key_paths(
        obj: Union[dict, list, tuple], cur_path: str = '',
        append_values: bool = False,
        paths: list = None, black_list: list = None):
    """
    Returns Python ready, full key paths as strings

    :param obj:
    :param cur_path: name of the variable that's being passed as the obj can be
    passed here to create eval ready key paths
    :param append_values: return corresponding key values along with the keys
    :param paths: the list that will contain all the found key paths, no need
    to pass anything
    :param black_list: dictionary keys which will be ignored (not paths)
    :return:
    """
    if paths is None:
        paths = []

    if isinstance(obj, (dict, list, tuple)):
        if isinstance(obj, dict):
            for key in obj:
                new_path = cur_path + f'[\'{key}\']'
                if isinstance(obj[key], dict):
                    if black_list is not None and key in black_list:
                        continue
                    get_final_key_paths(
                        obj[key], new_path, append_values, paths, black_list)
                elif isinstance(obj[key], (list, tuple)):
                    get_final_key_paths(obj[key], new_path,
                                        append_values, paths, black_list)
                else:
                    if append_values:
                        to_append = [new_path, obj[key]]
                    else:
                        to_append = new_path
                    paths.append(to_append)
        else:
            key_added = False  # same as in get_final_keys function
            for i in range(len(obj)):
                if isinstance(obj[i], (dict, tuple, list)):
                    get_final_key_paths(
                        obj[i], cur_path + f'[{i}]', append_values,
                        paths, black_list)
                else:
                    if not key_added:
                        if append_values:
                            to_append = [cur_path, obj]
                        else:
                            to_append = cur_path
                        paths.append(to_append)
                        key_added = True

    return paths


def logging_config(log_file_path: str,
                   file_level: int = logging.DEBUG,
                   console_level: int = logging.WARNING):
    """    Sets basicConfig - formatting, levels, adds a file and stream
    handlers.

    :param log_file_path: path to the log file
    :param file_level: logging threshold for the file handler
    :param console_level: logging threshold for the console handler
    :return:
    """
    msg_format = logging.Formatter('%(asctime)s {%(name)s %(funcName)s} '
                                   '%(levelname)s: %(message)s',
                                   datefmt='%Y-%m-%d %H:%M:%S')
    file_handler = handlers.RotatingFileHandler(log_file_path, 'a',
                                                (1024**2)*3, 5)
    file_handler.setLevel(file_level)
    file_handler.setFormatter(msg_format)
    console_out = logging.StreamHandler(stream=sys.stdout)
    console_out.setLevel(console_level)
    console_out.setFormatter(msg_format)
    logging.basicConfig(format=msg_format, level=file_level,
                        handlers=[file_handler, console_out])


def sqlite_connection(db_path: str, **kwargs) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, **kwargs)
    cur = conn.cursor()
    cur.execute('PRAGMA foreign_keys=ON;')
    conn.commit()
    cur.close()
    return conn


def load_file(path: str):
    with open(path, 'r') as file:
        return file.read()


def write_to_file(path: str, content):
    with open(path, 'r') as file:
        file.write(content)
