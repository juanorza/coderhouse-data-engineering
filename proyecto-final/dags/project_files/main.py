from datetime import datetime, date
from functools import partial, reduce
import requests
import pandas as pd

from project_files import extract, transform, load
from project_files.start_db import engine, start_db

"""
    Script para ejecutar la descarga, transformación y carga de mediciones climáticas diarias 
    para algunas ciudades de la Argentina.
    
    La información se descarga desde https://open-meteo.com/ 
    La carga se hace en una base de datos de Amazon Redshift.
"""

DAILY_VARIABLES = [
    "weathercode",
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "sunrise",
    "sunset",
]

ETL_CONFIG = {
    "extract": {
        "id": "ciudades_daily",
        "frequency": "daily",  # {daily, hourly}
        "query_params": {
            "start_date": None,
            "end_date": None,
            "daily": ",".join(DAILY_VARIABLES),
            "latitude": None,
            "longitude": None,
            "timezone": None,
        },
        "city_id": None,
    },
    "transform": {
        0: transform.corregir_tipos_mediciones_diarias,
        1: partial(transform.remover_duplicados, index_keys=["city_id", "time"]),
    },
    "load": {"table_name": "mediciones_diarias", "if_exists": "append"},
}


def main(extract_date: str) -> pd.DataFrame:
    """
    Ejecuta los flujos de extracción, transformación y carga para
    el objeto de configuración definido en la lista ETL_CONFIGS
    Args:
        start_date: dict
        end_date: dict
    Returns:
        df: el dataframe insertado en la base
    """

    # hacemos una copia fresca del objeto de configuración
    etl_config = ETL_CONFIG.copy()

    # modificamos start_date y end_date en el objeto de configuración
    etl_config["extract"]["query_params"]["start_date"] = extract_date
    etl_config["extract"]["query_params"]["end_date"] = extract_date

    # extract
    print("\nExtrayendo")
    df: pd.DataFrame = extract.extraer_dataframe(etl_config["extract"])

    ### transform ###
    # Se remueven filas duplicadas del dataframe tomando como referencia
    # un subconjunto de columnas que juega el rol de clave primaria.
    # Se ajustan los tipos de dato del dataframe para adaptarse a las columnas
    # de la base de datos.
    df = df
    print("\nTransformando")
    for k, transf_fun in etl_config.get("transform", {}).items():
        print("{} Transformación {}".format("-" * 5, k))
        df = transf_fun(df)

    ### load ###
    print("\nCargando en redshift")
    load.cargar_df_en_postgres(
        table_name=etl_config.get("load").get("table_name"),
        df=df,
        engine=engine,
        if_exists=etl_config.get("load").get("if_exists"),
    )

    return


if __name__ == "__main__":
    # la función start_db ejecuta DROP y CREATE sobre la tabla
    # que vamos a estar utilizando
    start_db()

    # la función main corre el proceso completo de ETL
    # df = main()

    # print("Revisamos las ultimas 5 filas insertadas")
    # for row in df.tail().iterrows():
    #     print(row, end="\n{}\n".format("-" * 5))
