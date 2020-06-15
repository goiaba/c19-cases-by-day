import logging
import mysql.connector as mariadb
from config import LOG_LEVEL

logging.basicConfig(level=LOG_LEVEL)


class MariaDBHandler(object):

    def __init__(self, host: str, database: str, user: str, password: str):
        self._host = host
        self._database = database
        self._user = user
        self._password = password

    def batch_insert(self, data: list):
        if data:
            stmt = """INSERT INTO covid_cases_history (idCountry, idState, idCity, cases, entranceDate)
                           VALUES (%(idCountry)s, %(idState)s, %(idCity)s, %(dailyCasesGrowth)s, %(entranceDate)s)"""
            self._batch_executor(stmt, data)

    def batch_update(self, data: list):
        if data:
            stmt = """UPDATE covid_cases_history
                         SET cases = %(dailyCasesGrowth)s, entranceDate = %(entranceDate)s
                       WHERE idCity = %(idCity)s
                         AND entranceDate = %(entranceDateToUpdate)s"""
            self._batch_executor(stmt, data)

    def get_latest_and_previous_entrance_date(self) -> tuple:
        query = "SELECT MAX(entranceDate) FROM covid_cases_history"
        records = self._query_executor(query)
        latest = records[0][0] if records[0][0] else None
        previous = None
        if latest:
            query = """SELECT MAX(entranceDate)
                         FROM covid_cases_history
                        WHERE entranceDate < (SELECT MAX(entranceDate) FROM covid_cases_history)"""
            records = self._query_executor(query)
            previous = records[0][0] if records[0][0] else None
        return previous, latest

    def _query_executor(self, query: str, data: tuple = None) -> list:
        connection: mariadb.connector = None
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
            if connection and connection.is_connected():
                connection.close()
                cursor.close()

    def _batch_executor(self, statement: str, data: list):
        connection: mariadb.connector = None
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
            if connection and connection.is_connected():
                connection.close()
                cursor.close()
