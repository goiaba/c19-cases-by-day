import csv
import logging
import os
import re
from config import CSV_FILENAME, ZIP_FILENAME_PATTERN, LOG_LEVEL
from datetime import datetime
from functools import reduce
from zipfile import ZipFile
from util import time_to_fsstr, fsstr_to_time

logging.basicConfig(level=LOG_LEVEL)


def _extract_csv_from_zip(zip_filename: str, dest_path: str) -> str:
    with ZipFile(zip_filename, "r") as zipObj:
        zipObj.extract(CSV_FILENAME, dest_path)
    return os.path.join(dest_path, CSV_FILENAME)


def _datetimes_from_filenames(filenames: list) -> filter:
    return filter(lambda e: e is not None, map(lambda e: datetime_from_filename(e), filenames))


def datetime_from_filename(filename: str) -> datetime:
    try:
        str_datetime = re.search(ZIP_FILENAME_PATTERN, filename).group(1)
        return fsstr_to_time(str_datetime)
    except AttributeError:
        return None


class FSHandler(object):
    def __init__(self, path: str, previous_datetime: datetime = None, start_datetime: datetime = None):
        self._path: str = path
        self._start_datetime: datetime = start_datetime if start_datetime else datetime.min
        self._previous_datetime: datetime = previous_datetime
        self._files_to_process: list = self._populate_files_to_process_list()

    def get_files_to_process(self) -> list:
        return self._files_to_process

    def _populate_files_to_process_list(self):
        files_dt = list(filter(lambda e: e > self._start_datetime, _datetimes_from_filenames(os.listdir(self._path))))
        if not files_dt:
            logging.info(f"There is no CSV file to process since {time_to_fsstr(self._start_datetime)}")
            return []
        # we reduce the list of files datetimes listed from the CSV directory to only one for each day. For instance, if
        #  the list contains csv_dados_31_05_2020-05_00_02.zip, ... and csv_dados_31_05_2020-22_59_01.zip the result
        #  list must contain only the file with the latest date in name: csv_dados_31_05_2020-22_59_01.zip
        files_dt = sorted(files_dt, reverse=True)
        datetimes = reduce(lambda a, e: a+[e] if (e > a[-1] or e.date() < a[-1].date()) else a,
                           files_dt,
                           [files_dt[0]])
        # if the date of oldest datetime is equal to _start_datetime it means we are handling a same day update case
        if datetimes[0].date() == self._start_datetime.date():
            datetimes.append(self._previous_datetime)
        elif self._start_datetime != datetime.min:
            # we add to the list the last file used to populate the history table in order to use it as a baseline
            datetimes.append(self._start_datetime)
        return list(map(lambda e: os.path.join(self._path, f"csv_dados_{time_to_fsstr(e)}.zip"), sorted(datetimes)))

    def get_data_from_csv(self, zip_filename):
        csv_filename = _extract_csv_from_zip(zip_filename, "/tmp")
        with open(csv_filename) as csv_file:
            header = csv_file.readline()\
                .replace("\"", "")\
                .replace("\n", "")\
                .replace("confirmed", "cases")\
                .replace("lastUpdate", "entranceDate")\
                .split(",")
            csv_reader = csv.reader(csv_file, delimiter=",")
            attrs_index = {a: header.index(a) for a in ["idCountry", "idState", "idCity", "cases", "entranceDate"]}
            data_dict = {}
            for line_number, row in enumerate(csv_reader):
                data_dict[int(row[attrs_index["idCity"]])] = {a: row[v] for a, v in attrs_index.items()}
            logging.info(f"Processed {line_number} lines from filesystem.")
        return data_dict
