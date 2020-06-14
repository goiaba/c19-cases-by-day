import logging
import os
from util import time_to_fsstr, time_to_mdbstr, mdbstr_to_time
from datetime import datetime
from config import CSV_FILES_PATH, LOG_LEVEL
from fs_handler import FSHandler, datetime_from_filename
from mariadb_handler import MariaDBHandler


logging.basicConfig(level=LOG_LEVEL)
db_handler = MariaDBHandler(host=os.environ.get("DB_HOST", "localhost"), database=os.environ.get("DB_NAME", "covid"),
                            user=os.environ.get("DB_USER", "root"), password=os.environ.get("DB_PASS", "root"))
start_datetime: datetime = db_handler.get_latest_entrance_date()
fs_handler = FSHandler(CSV_FILES_PATH, time_to_fsstr(start_datetime) if start_datetime else None)
cur_data: dict = {}

files_to_process = fs_handler.get_files_to_process()

for file in files_to_process:
    logging.info(f"Processing file {file}.")
    new_data: dict = fs_handler.get_data_from_csv(file)
    file_datetime: str = time_to_mdbstr(datetime_from_filename(file))
    insert_data: list = []
    start_datetime_cases = {}
    if start_datetime and len(files_to_process) > 1 \
            and (start_datetime == mdbstr_to_time(file_datetime)) \
            and (start_datetime.date() == datetime_from_filename(files_to_process[1]).date()):
        start_datetime_cases = db_handler.get_latest_cases(start_datetime)
        db_handler.delete_by_entrance_date(start_datetime)

    for id_city, data in new_data.items():
        # since we are calculating the new cases based in the day before, we must discard the first day. So we
        #  just add the information to be used in the next iteration and leave
        if not cur_data.get(id_city, None):
            cur_data[id_city] = {}
            cur_data[id_city]["idCountry"] = data.get("idCountry")
            cur_data[id_city]["idState"] = data.get("idState")
            cur_data[id_city]["idCity"] = id_city
            cur_data[id_city]["cases"] = int(data.get("cases")) - start_datetime_cases.get(id_city, 0)
            cur_data[id_city]["entranceDate"] = file_datetime
            continue
        new_cases = int(data.get("cases")) - cur_data[id_city]["cases"]
        cur_data[id_city]["cases"] = int(data.get("cases"))
        cur_data[id_city]["entranceDate"] = file_datetime
        insert_data.append((
            cur_data[id_city]["idCountry"],
            cur_data[id_city]["idState"],
            cur_data[id_city]["idCity"],
            new_cases,
            cur_data[id_city]["entranceDate"]))
    db_handler.batch_insert(insert_data)
