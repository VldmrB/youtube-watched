import sys
import logging
from hashlib import sha3_256
from logging import handlers
from typing import Union


def is_video_url(candidate_str: str):
    return True if 'youtube.com/watch?' in candidate_str else False


def extract_video_id_from_url(url):
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
        paths: list = None, black_list: list = None,
        final_keys_only: bool = False):
    """
    Returns Python ready, full key paths as strings

    :param obj:
    :param cur_path: name of the variable that's being passed as the obj can be
    passed here to create eval ready key paths
    :param append_values: return corresponding key values along with the keys
    :param paths: the list that will contain all the found key paths, no need
    to pass anything
    :param black_list: dictionary keys which will be ignored
    :param final_keys_only: return only the final key from each path
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
                        obj[key], new_path, append_values, paths, black_list,
                        final_keys_only)
                elif isinstance(obj[key], (list, tuple)):
                    get_final_key_paths(
                        obj[key], new_path, append_values, paths, black_list,
                        final_keys_only)
                else:
                    if final_keys_only:
                        last_bracket = new_path.rfind('[\'')
                        new_path = new_path[
                                   last_bracket+2:new_path.rfind('\'')]
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
                        paths, black_list, final_keys_only)
                else:
                    if not key_added:
                        if final_keys_only:
                            last_bracket = cur_path.rfind('[\'')
                            cur_path = cur_path[
                                       last_bracket+2:cur_path.rfind('\'')]
                        if append_values:
                            to_append = [cur_path, obj]
                        else:
                            to_append = cur_path
                        paths.append(to_append)
                        key_added = True

    return paths


loggers_to_remove = ['werkzeug', 'flask']


def logging_config(log_file_path: str,
                   file_level: int = logging.DEBUG,
                   console_out_level: int = logging.DEBUG,
                   console_err_level: int = logging.WARNING,
                   native_app_logger_to_file=False):
    """    Sets basicConfig - formatting, levels, adds a file and stream
    handlers.

    :param log_file_path: path to the log file
    :param file_level: logging threshold for the file handler
    :param console_out_level: logging threshold for the console std handler
    :param console_err_level: logging threshold for the console err handler
    :param native_app_logger_to_file: enable app's native logging output to go
    to file as well
    :return:
    """

    class ConsoleOutFilter(logging.Filter):
        def __init__(self, level: int):
            super(ConsoleOutFilter).__init__()
            self.level = level

        def filter(self, record):
            return record.levelno <= self.level

    class BlackListFilter(logging.Filter):
        def __init__(self):
            super(BlackListFilter).__init__()

        def filter(self, record):
            for logger_name in loggers_to_remove:
                if record.name.startswith(logger_name):
                    return
            else:
                return True

    log_format = logging.Formatter('%(asctime)s {%(name)s.%(funcName)s} '
                                   '%(levelname)s: %(message)s',
                                   datefmt='%Y-%m-%d %H:%M:%S')
    std_format = logging.Formatter('%(asctime)s {%(funcName)s} '
                                   '%(levelname)s: %(message)s',
                                   datefmt='%H:%M:%S')
    file_handler = handlers.RotatingFileHandler(log_file_path, 'a',
                                                (1024**2)*3, 5)
    file_handler.setLevel(file_level)
    file_handler.setFormatter(log_format)
    if not native_app_logger_to_file:
        file_handler.addFilter(BlackListFilter())

    console_out = logging.StreamHandler(stream=sys.stdout)
    console_out.setLevel(console_out_level)
    console_out.setFormatter(std_format)
    console_out.addFilter(ConsoleOutFilter(logging.INFO))
    console_out.addFilter(BlackListFilter())

    console_err = logging.StreamHandler(stream=sys.stderr)
    console_err.setLevel(console_err_level)
    console_err.setFormatter(std_format)
    console_err.addFilter(ConsoleOutFilter(logging.CRITICAL))
    console_err.addFilter(BlackListFilter())

    logging.basicConfig(format=log_format, level=file_level,
                        handlers=[file_handler, console_out, console_err])


def load_file(path: str):
    with open(path, 'r') as file:
        return file.read()


def write_to_file(path: str, content):
    with open(path, 'r') as file:
        file.write(content)


def get_hash(obj: bytes) -> str:
    return sha3_256(obj).hexdigest()
