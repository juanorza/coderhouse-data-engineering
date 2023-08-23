import pandas as pd
from sqlalchemy.engine import Engine


def cargar_df_en_postgres(
    table_name: str, df: pd.DataFrame, engine: Engine, if_exists: str = "fail"
):

    print(f"Insertando {df.shape[0]} filas en {table_name}... (puede demorar)")
    rows = df.to_sql(name=table_name, con=engine, if_exists=if_exists, index=False)

    print(f"Insertadas {rows} filas")
