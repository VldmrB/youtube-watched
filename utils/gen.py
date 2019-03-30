import bisect
import logging
import sys
from datetime import datetime
from logging import handlers

from config import MAX_TIME_DIFFERENCE

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


def are_different_timestamps(ts1: datetime,
                             ts2: datetime) -> bool:
    """Since each archive could potentially have timestamps in a
    different timezone, the same ones from different files could
    show as multiple unique timestamps due to different day/hour

    This function doesn't attempt to make timestamps accurate, and it may
    block an extremely small number of legitimate ones from being
    entered. Mostly, it will block the duplicates, however"""
    if ts1.replace(day=1, hour=0) == ts2.replace(day=1, hour=0):
        return False
    return True


def remove_timestamps_from_one_list_from_another(filter_, filteree):
    """Useful for when Takeouts are added out of order and/or for
    when stories show up as normal videos in newer Takeouts"""
    for timestamp in filter_:
        start = bisect.bisect_left(filteree,
                                   timestamp - MAX_TIME_DIFFERENCE)
        end = bisect.bisect_right(filteree,
                                  timestamp + MAX_TIME_DIFFERENCE)
        if start != end:
            for potential_duplicate in range(start, end):
                if not are_different_timestamps(timestamp,
                                                filteree[potential_duplicate]):
                    filteree.pop(potential_duplicate)
                    break


def timestamp_is_unique_in_list(candidate, timestamps, insert=False):
    start = bisect.bisect_left(timestamps,
                               candidate - MAX_TIME_DIFFERENCE)
    end = bisect.bisect_right(timestamps,
                              candidate + MAX_TIME_DIFFERENCE)
    if start == end and start == 0:  # no similar records found
        if insert:
            bisect.insort_left(timestamps, candidate)
    else:
        for incumbent in range(start, end):
            if not are_different_timestamps(candidate, timestamps[incumbent]):
                return False
        else:
            if insert:
                bisect.insort_left(timestamps, candidate)
    return True


def load_file(path: str):
    with open(path, 'r') as file:
        return file.read()


def write_to_file(path: str, content):
    with open(path, 'w') as file:
        file.write(content)
