import logging
import mysql.connector as mariadb
from config import LOG_LEVEL
from datetime import datetime

logging.basicConfig(level=LOG_LEVEL)


class MariaDBHandler(object):

    def __init__(self, host: str, database: str, user: str, password: str):
        self._host = host
        self._database = database
        self._user = user
        self._password = password

    def batch_insert(self, data: list):
        stmt = """INSERT INTO covid_cases_history (idCountry, idState, idCity, cases, entranceDate)
                              VALUES (%s, %s, %s, %s, %s)"""
        self._batch_executor(stmt, data)

    def delete_by_entrance_date(self, entrance_date: datetime):
        stmt = """DELETE FROM covid_cases_history WHERE entranceDate = %s"""
        self._batch_executor(stmt, [(entrance_date,)])

    def get_latest_cases(self, start_datetime: datetime) -> dict:
        query = """SELECT idCity, cases FROM covid_cases_history WHERE entranceDate = %s"""
        records = self._query_executor(query, (start_datetime,))
        return {row[0]: row[1] for row in records}

    def get_latest_entrance_date(self) -> datetime:
        query = "SELECT MAX(entranceDate) FROM covid_cases_history"
        records = self._query_executor(query)
        return records[0][0] if records[0][0] else None

    def _query_executor(self, query: str, data: tuple = None):
        try:
            connection = mariadb.connect(
                host=self._host, database=self._database, user=self._user, password=self._password)
            cursor = connection.cursor()
            cursor.execute(query, data)
            return cursor.fetchall()
        except mariadb.Error as error:
            logging.error("Error reading data from MariaDB table.")
            raise error
        finally:
            if connection.is_connected():
                connection.close()
                cursor.close()

    def _batch_executor(self, statement: str, data: list):
        try:
            connection = mariadb.connect(
                host=self._host, database=self._database, user=self._user, password=self._password)
            cursor = connection.cursor()
            cursor.executemany(statement, data)
            connection.commit()
        except mariadb.Error as error:
            logging.error("Failed to execute batch statement.")
            raise error
        finally:
            if connection.is_connected():
                connection.close()
                cursor.close()
