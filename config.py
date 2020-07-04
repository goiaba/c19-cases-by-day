import logging

CSV_FILENAME = "covid_cases_by_city.csv"
CSV_FILES_PATH = "/home/bruno/CloudStorage/GitHub/Midev/web/rsync-var-www-html/var/www/html/repository/db/csvs/"
DATETIME_PATTERN = "%d_%m_%Y-%H_%M_%S"
MARIADB_DATETIME_PATTERN = "%Y-%m-%d %H:%M:%S"
ZIP_FILENAME_PATTERN = "csv_dados_(.._.._20..-.._.._..)\.zip"
LOG_LEVEL = logging.DEBUG
PLOT = {
    "images_output_path": "images",
    "images_tmp_path": "/tmp",
    "shape_files_path": "shapefiles"
}
