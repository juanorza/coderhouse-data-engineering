from datetime import datetime, date
import requests
import pandas as pd
from functools import partial, reduce

import extract, transform, load
from start_db import engine, start_db

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

ETL_CONFIGS = [
    {
        "extract": {
            "id": "ciudades_daily_202301",
            "frequency": "daily",  # {daily, hourly}
            "query_params": {
                "start_date": "2023-01-01",  # limitamos las fechas para el ejecicio (enero 2023)
                "end_date": "2023-01-31",
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
        "load": {"table_name": "mediciones_diarias", "if_exists": "replace"},
    }
]


def main() -> pd.DataFrame:
    """
    Ejecuta los flujos de extracción, transformación y carga para
    el objeto de configuración definido en la lista ETL_CONFIGS
    Returns:
        df: el dataframe insertado en la base
    """
    etl_config = ETL_CONFIGS[0]

    # extract
    print("\nExtrayendo")
    df: pd.DataFrame = extract.extraccion(etl_config["extract"])

    ### transform ###
    # Se remueven filas duplicadas del dataframe tomando como referencia
    # un subconjunto de columnas que juega el rol de clave primaria.
    # Se ajustan los tipos de dato del dataframe para adaptarse a las columnas
    # de la base de datos.
    print("\nTransformando")
    for k, transf_fun in etl_config.get("transform", {}).items():
        print(f"{'-'*5} Transformación {k}")
        df = transf_fun(df)

    ### load ###
    print("\nCargando en redshift")
    load.cargar_df_en_postgres(
        table_name=etl_config.get("load").get("table_name"),
        df=df,
        engine=engine,
        if_exists=etl_config.get("load").get("if_exists"),
    )

    return df


if __name__ == "__main__":
    # la función start_db ejecuta DROP y CREATE sobre la tabla
    # que vamos a estar utilizando
    start_db()

    # la función main corre el proceso completo de ETL
    df = main()

    print("Revisamos las ultimas 5 filas insertadas")
    for row in df.tail().iterrows():
        print(row, end=f"\n{'-'*5}\n")
