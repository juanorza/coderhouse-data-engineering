import pandas as pd
from airflow.decorators import task


@task
def lanzar_alerta_task(csv_filename: str, ds=None):
    """
    #### Lanzar Alerta task
    Combina el documento ciudades.csv con la data descargada, para detectar posibles alertas según lo configurado.
    Las alertas pueden visualizarse en el log de ejecución de la tarea.
    """
    print("alerta! {}".format(csv_filename))

    df_ciudades = pd.read_csv("dags/project_files/ciudades.csv")
    df_data = pd.read_csv(csv_filename)
    df_data = df_data.loc[df_data.time == ds]

    df_ciudades = pd.merge(df_ciudades, df_data, how="inner", on="city_id")

    alertas = ""
    for c in df_ciudades.to_dict(orient="records"):
        city_id = c.get("city_id")
        temp_min = c.get("temperature_2m_min")
        temp_max = c.get("temperature_2m_max")
        alerta_min = c.get("alerta_min")
        alerta_max = c.get("alerta_max")

        if temp_min < alerta_min:
            alertas += f"\n{' '*5}City ID: {city_id}\ttemperatura mínima {temp_min}°C inferior a la alerta de {alerta_min}°C"

        if temp_max > alerta_max:
            alertas += f"\n{' '*5}City ID: {city_id}\ttemperatura máxima {temp_max}°C superior a la alerta de {alerta_max}°C"

    if alertas:
        print(
            f"""
              \n\n{'*'*4} ALERTAS {ds} {'*'*4}
              {alertas}
              """,
        )
    else:
        print(f"\n\n{'*'*4} SIN ALERTAS {'*'*4}\n\n")
