from typing import List, Optional

import pandas as pd
import requests


BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

DAILY_VARIABLES = [
    "weathercode",
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "sunrise",
    "sunset",
]

EXCTRACT_CONFIGS = [
    {
        "id": "ciudades_daily_2022",
        "frequency": "daily",  # hourly
        "query_params": {
            "start_date": "2022-01-01",
            "end_date": "2022-12-31",
            "daily": ",".join(DAILY_VARIABLES),
            "latitude": None,
            "longitude": None,
            "timezone": None,
        },
        "city_id": None,
    }
]

CIUDADES = [
    {
        "city_id": "buenos_aires",
        "nombre": "Buenos Aires",
        "latitude": "-34.6",
        "longitude": "-58.4",
        "timezone": "GMT",
    },
    {
        "city_id": "el_calafate",
        "nombre": "El Calafate",
        "latitude": "-50.3",
        "longitude": "-72.3",
        "timezone": "GMT",
    },
    {
        "city_id": "posadas",
        "nombre": "Posadas",
        "latitude": "-27.4",
        "longitude": "-55.9",
        "timezone": "GMT",
    },
    {
        "city_id": "san_luis",
        "nombre": "San Luis",
        "latitude": "-33.3",
        "longitude": "-66.3",
        "timezone": "GMT",
    },
    {
        "city_id": "san_juan",
        "nombre": "San Juan",
        "latitude": "-31.5",
        "longitude": "-68.5",
        "timezone": "GMT",
    },
    {
        "city_id": "cordoba",
        "nombre": "Córdoba",
        "latitude": "-34.6",
        "longitude": "-58.4",
        "timezone": "GMT",
    },
    {
        "city_id": "rosario",
        "nombre": "Rosario",
        "latitude": "-32.9",
        "longitude": "-60.6",
        "timezone": "GMT",
    },
    {
        "city_id": "mendoza",
        "nombre": "Mendoza",
        "latitude": "-32.9",
        "longitude": "-68.8",
        "timezone": "GMT",
    },
    {
        "city_id": "san_miguel_de_tucuman",
        "nombre": "San Miguel de Tucumán",
        "latitude": "-26.8",
        "longitude": "-65.2",
        "timezone": "GMT",
    },
    {
        "city_id": "la_plata",
        "nombre": "La Plata",
        "latitude": "-34.9",
        "longitude": "-58.0",
        "timezone": "GMT",
    },
    {
        "city_id": "salta",
        "nombre": "Salta",
        "latitude": "-24.8",
        "longitude": "-65.4",
        "timezone": "GMT",
    },
    {
        "city_id": "resistencia",
        "nombre": "Resistencia",
        "latitude": "-27.5",
        "longitude": "-59.0",
        "timezone": "GMT",
    },
    {
        "city_id": "santiago_del_estero",
        "nombre": "Santiago del Estero",
        "latitude": "-27.8",
        "longitude": "-64.3",
        "timezone": "GMT",
    },
    {
        "city_id": "san_salvador_de_jujuy",
        "nombre": "San Salvador de Jujuy",
        "latitude": "-24.2",
        "longitude": "-65.3",
        "timezone": "GMT",
    },
    {
        "city_id": "parana",
        "nombre": "Paraná",
        "latitude": "-31.7",
        "longitude": "-58.7",
        "timezone": "GMT",
    },
    {
        "city_id": "rio_gallegos",
        "nombre": "Río Gallegos",
        "latitude": "-51.6",
        "longitude": "-69.2",
        "timezone": "GMT",
    },
    {
        "city_id": "rawson",
        "nombre": "Rawson",
        "latitude": "-43.3",
        "longitude": "-65.1",
        "timezone": "GMT",
    },
    {
        "city_id": "viedma",
        "nombre": "Viedma",
        "latitude": "-40.8",
        "longitude": "-63.0",
        "timezone": "GMT",
    }
]


def descargar_url_clima(url: str):
    req = requests.get(url)
    req.raise_for_status()

    return req.json()


def armar_url_con_parametros(url_base: str, parametros: dict) -> str:
    return_url = url_base + "/?"
    for param_key, param_value in parametros.items():
        return_url += f"&{param_key}={param_value}"

    return return_url


def extraer_configuracion(config: dict) -> pd.DataFrame:
    print(f"Extrayendo configuración id={config['id']}, city_id={config['city_id']}")

    url = armar_url_con_parametros(BASE_URL, config.get("query_params"))

    descarga = None
    try:
        descarga_dict = descargar_url_clima(url)
        descarga = descarga_dict.get(config.get("frequency"))
    except Exception as e:
        print(f"{url= }")
        print(e)

    if not descarga:
        return pd.DataFrame()

    df_descarga = pd.DataFrame(descarga)
    df_descarga.insert(0, "city_id", config["city_id"])
    # df_descarga["city_id"] = config["city_id"]
    print(f"{' '*5} {df_descarga.shape[0]} filas descargadas")
    return df_descarga


def extraccion(extract_config: dict) -> pd.DataFrame:
    """
    Ejecuta la descarga del objeto de configuración extract_config,
    repitiendo la tarea para cada una de las ciudades del listado CIUDADES
    Args:
        extract_config: dict
    Returns:

    """
    df_resultado = pd.DataFrame()

    for ciudad in CIUDADES:
        # Completar objeto de configuración
        config_c = extract_config.copy()
        config_c["city_id"] = ciudad.get("city_id")
        config_c["query_params"]["latitude"] = ciudad.get("latitude")
        config_c["query_params"]["longitude"] = ciudad.get("longitude")
        config_c["query_params"]["timezone"] = ciudad.get("timezone")

        df_ciudad: pd.DataFrame = extraer_configuracion(config_c)
        if df_ciudad.empty:
            continue

        if df_resultado.empty:
            df_resultado = df_ciudad
        else:
            df_resultado = pd.concat([df_resultado, df_ciudad], ignore_index=True)

    return df_resultado
