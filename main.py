import logging
import os
from util import time_to_mdbstr
from config import CSV_FILES_PATH, LOG_LEVEL
from fs_handler import FSHandler, datetime_from_filename, get_data_from_csv
from mariadb_handler import MariaDBHandler
from plot_handler import PlotHandler


logging.basicConfig(level=LOG_LEVEL)
db_handler = MariaDBHandler(host=os.environ.get("DB_HOST", "localhost"), database=os.environ.get("DB_NAME", "covid"),
                            user=os.environ.get("DB_USER", "root"), password=os.environ.get("DB_PASS", "root"))
previous_datetime, start_datetime = db_handler.get_latest_and_previous_entrance_date()
fs_handler = FSHandler(CSV_FILES_PATH, previous_datetime, start_datetime)
cur_data: dict = {}

pl_handler = PlotHandler(db_handler=db_handler)

for file in fs_handler.get_files_to_process():
    logging.info(f"Processing file {file}.")
    new_data: dict = get_data_from_csv(file)
    file_datetime: str = time_to_mdbstr(datetime_from_filename(file))
    db_data: list = []
    for id_city, data in new_data.items():
        # since we are calculating the new cases based in the day before, we must discard the first day. So we
        #  just add the information to be used in the next iteration and leave
        if not cur_data.get(id_city, None):
            cur_data[id_city] = {}
            cur_data[id_city]["idCountry"] = data.get("idCountry")
            cur_data[id_city]["idState"] = data.get("idState")
            cur_data[id_city]["idCity"] = id_city
            cur_data[id_city]["cases"] = int(data.get("cases"))
            cur_data[id_city]["entranceDate"] = file_datetime
            continue
        cur_data[id_city]["entranceDateToUpdate"] = start_datetime
        cur_data[id_city]["dailyCasesGrowth"] = int(data.get("cases")) - cur_data[id_city]["cases"]
        cur_data[id_city]["cases"] = int(data.get("cases"))
        cur_data[id_city]["entranceDate"] = file_datetime
        db_data.append(cur_data[id_city].copy())
    if start_datetime and datetime_from_filename(file).date() == start_datetime.date():
        db_handler.batch_update(db_data)
    else:
        db_handler.batch_insert(db_data)
    pl_handler.save_images(file_datetime)
