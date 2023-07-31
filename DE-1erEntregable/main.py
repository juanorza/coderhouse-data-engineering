import requests
import pandas as pd

"""
    Script para extraer algunas mediciones climáticas por hora, otras por día
    en formato JSON, para un listado determinado de ciudades.
    
    La información se descarga desde https://open-meteo.com/
    
    En esta primer entrega, se descargan algunas variables de frecuencia horaria y otras de frecuencia diaria, para ser almacenadas en tablas de redshift.
"""

CIUDADES = {
    'buenos_aires': {
        'nombre': 'Buenos Aires',
        'latitude': '-34.6',
        'longitude': '-58.4',
        'timezone': 'GMT'
    },
    'el_calafate': {
        'nombre': 'El Calafate',
        'latitude': '-50.3',
        'longitude': '-72.3',
        'timezone': 'GMT'
    }
}

BASE_URL = 'https://archive-api.open-meteo.com/v1/archive'

HOURLY_VARIABLES = ['temperature_2m', 'apparent_temperature', 'precipitation', 'cloudcover', 'cloudcover_low',
                    'cloudcover_mid', 'cloudcover_high', 'windspeed_10m', 'winddirection_10m']

DAILY_VARIABLES = ['weathercode', 'temperature_2m_max', 'temperature_2m_min', 'temperature_2m_mean', 'sunrise',
                   'sunset']


def descargar_url_clima(
        url: str):
    req = requests.get(url)
    req.raise_for_status()

    return req.json()


def descargar_ciudad_entre_fechas(
        city_dict, start_date, end_date) -> dict:
    """
    Arma la URL de descarga en base a los argumentos
    y llama a la función que descarga la info
    Args:
        city_dict:
        start_date:
        end_date:

    Returns: Resultados crudos del API

    """
    print(f"Descargando mediciones para {city_dict['nombre']} \nentre fechas {start_date=} y {end_date=}")
    url = BASE_URL + "/?"
    for param_key in ['latitude', 'longitude', 'timezone']:
        url += f"&{param_key}={city_dict[param_key]}"

    url += f"&start_date={start_date}"
    url += f"&end_date={end_date}"
    url += f"&hourly={','.join(HOURLY_VARIABLES)}"
    url += f"&daily={','.join(DAILY_VARIABLES)}"

    try:
        return descargar_url_clima(url)
    except Exception as e:
        print(f"{url= }")
        print(e)
        return {}


def procesar_resultados(
        city_id, resultados_ciudad: dict) -> dict:
    """
    Procesa los resultados del api https://archive-api.open-meteo.com/v1/archive
    para obtener un listado de diccionarios con los valores de cada variable para cada período de tiempo

    Args:
        city_id: nombre código de la ciudad
        resultados_ciudad: el resultado de una búsqueda con la funcion descargar_ciudad_entre_fechas
    """

    filas_procesadas = {}
    for freq in ('hourly', 'daily'):
        freq_dict = resultados_ciudad[freq]

        # usamos Pandas para pasar toda la información columnar a un dataframe, para después exportar las filas
        df = pd.DataFrame(freq_dict)
        df['city_id'] = city_id
        filas_procesadas[freq] = df.to_dict(orient='records')

    return filas_procesadas


def descargar_todas_ciudades_entre_fechas_procesado(
        start_date: str, end_date: str) -> dict:
    """
    Gestiona la descarga de la información entre las fechas dadas,
    para las variables por hora y por día seleccionadas,
    para cada ciudad del diccionario CIUDADES

    Args:
        start_date: 'YYYY-MM-DD'
        end_date: 'YYYY-MM-DD'
    """

    resultados = {}
    for city_id, city_dict in CIUDADES.items():
        resultados_crudos = descargar_ciudad_entre_fechas(city_dict, start_date, end_date)
        resultados_procesados = procesar_resultados(city_id, resultados_crudos)

        resultados[city_id] = resultados_procesados

    return resultados


if __name__ == '__main__':
    START_DATE = '2023-07-01'
    END_DATE = '2023-07-13'

    print(f"{'*' * 7} Empezando descarga {'*' * 7}")

    descarga = descargar_todas_ciudades_entre_fechas_procesado(START_DATE, END_DATE)

    print(f"{'*' * 7} Descarga Finalizada {'*' * 7}")

    for city_id, city_values in descarga.items():
        for freq in ('hourly', 'daily'):
            print(f"\n{'/' * 6} {city_id} | {freq}: {len(city_values[freq])} resultados.\nMostrando primeros 5:")
            df = pd.DataFrame(city_values[freq])
            print(f"{df.shape[0]} filas, {df.shape[1]} columnas")
            print(df.head().to_string())
