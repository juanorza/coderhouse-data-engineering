from functools import partial
import pandas as pd
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine
from airflow.decorators import task
from datetime import datetime
from project_files.start_db import engine


def insert_on_conflict_nothing(table, conn, keys, data_iter):
    # "a" is the primary key in "conflict_table"
    data = [dict(zip(keys, row)) for row in data_iter]
    insert_stmt = insert(table.table).values(data)
    do_nothing_stmt = insert_stmt.on_conflict_do_nothing(
        # index_elements=["city_id", "time"]
    )

    result = conn.execute(do_nothing_stmt)
    return result.rowcount


def cargar_df_en_postgres(
    table_name: str, df: pd.DataFrame, engine: Engine, if_exists: str = "fail"
):
    print(
        "Insertando {} filas en {}... (puede demorar)".format(df.shape[0], table_name)
    )
    rows = df.to_sql(
        name=table_name,
        con=engine,
        if_exists=if_exists,
        index=False,
        method="multi",  # insert_on_conflict_nothing # "multi",
    )

    print("Insertadas {} filas".format(rows))

    return df


@task
def transform_and_load_task(csv_filename: str, load_config: dict, ds=None):
    df = pd.read_csv(csv_filename)

    # Se aplican las transformaciones necesarias
    from project_files import transform as tf

    # se corrigen los tipos de dato
    df = tf.corregir_tipos_mediciones_diarias(df)
    # se simula una corrección de posibles filas duplicadas
    df = tf.remover_duplicados(df, index_keys=["city_id", "time"])

    # Previo a la carga, borramos la información que pudiese haber guardada sobre esa fecha
    with engine.begin() as conn:
        conn.execute(
            f"DELETE FROM {load_config['table_name']} WHERE time = '{datetime.strptime(ds, '%Y-%m-%d').date()}';"
        )

    cargar_df_en_postgres(
        table_name=load_config.get("table_name"),
        df=df,
        engine=engine,
        if_exists=load_config.get("if_exists"),
    )
