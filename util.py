import config
import logging
from datetime import datetime

logging.basicConfig(level=config.LOG_LEVEL)


def str_to_time(str_time: str, time_pattern: str):
    return datetime.strptime(str_time, time_pattern)


def time_to_str(time: datetime, time_pattern: str):
    return datetime.strftime(time, time_pattern)


def mdbstr_to_time(str_time: str):
    return str_to_time(str_time, config.MARIADB_DATETIME_PATTERN)


def time_to_mdbstr(time: datetime):
    return time_to_str(time, config.MARIADB_DATETIME_PATTERN)


def fsstr_to_time(str_time: str):
    return str_to_time(str_time, config.DATETIME_PATTERN)


def time_to_fsstr(time: datetime):
    return time_to_str(time, config.DATETIME_PATTERN)
