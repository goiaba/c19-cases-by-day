import geopandas as gpd
import geoplot as gplt
import logging
import matplotlib.pyplot as plt
import os
import pandas
from config import LOG_LEVEL
from datetime import datetime
from fs_handler import zip_directory_and_rm_src, get_country_shape_file_path, get_state_shape_file_path,\
    get_images_tmp_filename, create_images_tmp_path, get_images_tmp_path, get_zip_file_name
from mariadb_handler import MariaDBHandler
from shapely.geometry import Point


logging.basicConfig(level=LOG_LEVEL)


state_name_to_initial = {
    "ACRE": "AC",
    "ALAGOAS": "AL",
    "AMAZONAS": "AM",
    "AMAPÁ": "AP",
    "BAHIA": "BA",
    "CEARÁ": "CE",
    "DISTRITO FEDERAL": "DF",
    "ESPÍRITO SANTO": "ES",
    "GOIÁS": "GO",
    "MARANHÃO": "MA",
    "MATO GROSSO": "MT",
    "MATO GROSSO DO SUL": "MS",
    "MINAS GERAIS": "MG",
    "PARÁ": "PA",
    "PARAÍBA": "PB",
    "PERNAMBUCO": "PE",
    "PIAUÍ": "PI",
    "RIO DE JANEIRO": "RJ",
    "RIO GRANDE DO NORTE": "RN",
    "RIO GRANDE DO SUL": "RS",
    "RONDÔNIA": "RO",
    "RORAIMA": "RR",
    "SANTA CATARINA": "SC",
    "SÃO PAULO": "SP",
    "SERGIPE": "SE",
    "TOCANTINS": "TO",
    "PARANÁ": "PR"
}


def _save_image(shape: gpd.GeoDataFrame, data: gpd.GeoDataFrame, output_file: str):
    fig, ax = plt.subplots(figsize=(6, 6))
    gplt.polyplot(shape, ax=ax, zorder=1)
    gplt.pointplot(data, color="red", s=.5, ax=ax, zorder=2)
    shape_bounds = shape.total_bounds
    ax.set_ylim(shape_bounds[1], shape_bounds[3])
    ax.set_xlim(shape_bounds[0], shape_bounds[2])
    logging.info(f"Saving image to {output_file}")
    plt.savefig(output_file, bbox_inches='tight', pad_inches=0.1, dpi=300)
    # TODO: Solve "RuntimeWarning: More than 20 figures have been opened."
    plt.clf()


class PlotHandler(object):

    def __init__(self, host: str, database: str, user: str, password: str, db_handler: MariaDBHandler = None):
        if db_handler:
            self._db_handler = db_handler
        else:
            self._db_handler = MariaDBHandler(host, database, user, password)

    def save_images(self, entrance_date: str):
        df = self._create_df(entrance_date)
        if df.empty:
            logging.info(f"Empty result returned to the defined entrance_date ('{entrance_date}').")
        else:
            create_images_tmp_path(entrance_date)
            brl_cases = gpd.GeoDataFrame(df)
            brl_states_shape = gpd.read_file(get_country_shape_file_path())
            _save_image(brl_states_shape, brl_cases, get_images_tmp_filename(entrance_date, "BR"))
            # TODO: Replace this naive implementation by something that performs better.
            for state_idx, state in brl_states_shape.iterrows():
                state_initials = state_name_to_initial.get(state['NM_ESTADO'])
                state_shape = gpd.read_file(get_state_shape_file_path(state_initials, state['CD_GEOCUF']))
                county_cases_filter = pandas.Series(
                    map(lambda e: state["geometry"].contains(e[1]["geometry"]), brl_cases.iterrows()),
                    brl_cases.index)
                _save_image(state_shape, brl_cases[county_cases_filter],
                            get_images_tmp_filename(entrance_date, state_initials))
            zip_directory_and_rm_src(get_images_tmp_path(entrance_date), get_zip_file_name(entrance_date))

    def _create_df(self, entrance_date: datetime) -> pandas.DataFrame:
        df = self._db_handler.get_cases_by_entrance_date(entrance_date)
        if not df.empty:
            df["geometry"] = [Point(xy) for xy in zip(df["longitude"], df["latitude"])]
            df["rm_cases"] = df["cases"].rolling(window=3).mean()
            df = df.query("rm_cases > 0")
            return df.drop(["latitude", "longitude", "rm_cases"], axis=1)
        return None
