import pandas as pd
from typing import List
from datetime import datetime


def remover_duplicados(df: pd.DataFrame, index_keys: List[str]) -> pd.DataFrame:
    """
    Remueve las filas que contengan valores duplicados para el listado de columnas `index_keys`
    Args:
        df
        index_keys
    """
    print(
        f"Removiendo filas duplicadas en {index_keys}",
    )
    rows_0 = df.shape[0]
    df.drop_duplicates(subset=index_keys, keep="first", inplace=True)
    rows_1 = df.shape[0]
    if (diff := rows_0 - rows_1) > 0:
        print(f"{diff} filas duplicadas removidas!")
    else:
        print(f"No se encontraron filas duplicadas.")

    return df


def corregir_tipos_mediciones_diarias(df: pd.DataFrame) -> pd.DataFrame:
    """
    Corrige los tipos de datos para la descarga de información que va hacia la tabla `mediciones_diarias`
    Args:
        df:
    """
    print("Ajustando tipo de datos")
    df["time"] = df["time"].apply(
        lambda x: datetime.strptime(x[:10], "%Y-%m-%d").date()
    )
    df["sunrise"] = df["sunrise"].apply(
        lambda x: datetime.strptime(x[:16], "%Y-%m-%dT%H:%M")
    )
    df["sunset"] = df["sunset"].apply(
        lambda x: datetime.strptime(x[:16], "%Y-%m-%dT%H:%M")
    )

    return df
