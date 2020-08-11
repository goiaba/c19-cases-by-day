import pandas
from mariadb_handler import MariaDBHandler


class LstmDao(MariaDBHandler):
    def __init__(self, host: str, database: str, user: str, password: str):
        super(LstmDao, self).__init__(host, database, user, password)

    def persist_cases_prediction(self, data: list):
        if data:
            stmt = """INSERT INTO covid_cases_prediction_by_country (idCountry, predictedCases, datePrediction, entranceDate)
                           VALUES (%(idCountry)s, %(predictedCases)s, %(datePrediction)s, %(entranceDate)s)"""
            self._batch_executor(stmt, data)

    def delete_cases_prediction_if_exists(self, entrance_date_str):
        stmt = """DELETE FROM covid_cases_prediction_by_country
                        WHERE entranceDate = %(entranceDate)s"""
        self._batch_executor(stmt, [{"entranceDate": entrance_date_str}])
