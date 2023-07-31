from sqlalchemy import create_engine

user = 'orza_juan_coderhouse'
password = '0p1ZpL3p3x'
host = 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
port = '5439'
database = 'data-engineer-database'

db_string = f"redshift+psycopg2://{user}:{password}@{host}:{port}/{database}"
engine = create_engine(db_string, echo=True)

# Definimos las tablas para almacenar las mediciones descargadas
table_names = ('mediciones_horarias', 'mediciones_diarias')

# Eliminamos las tablas, para hacer un comienzo nuevo
comandos_drop = [
    f"DROP TABLE IF EXISTS {table_name}" for table_name in table_names
]

# Comandos SQL para crear las tablas nuevamente
comandos_create = []
comandos_create.append(
    '''
    CREATE TABLE mediciones_horarias (
        city_id VARCHAR(60),
        time TIMESTAMP,
        temperature_2m REAL,
        apparent_temperature REAL,
        precipitation REAL,
        cloudcover REAL,
        cloudcover_low REAL,
        cloudcover_mid REAL,
        cloudcover_high REAL,
        windspeed_10m REAL,
        winddirection_10m REAL    
    )
    '''
)

comandos_create.append(
    '''
    CREATE TABLE mediciones_diarias (
        city_id VARCHAR(60),
        time DATE,
        weathercode REAL,
        temperature_2m_max REAL,
        temperature_2m_min REAL,
        temperature_2m_mean REAL,
        sunrise TIMESTAMP,
        sunset TIMESTAMP
    )
    '''
)

if __name__ == '__main__':
    # Ejecutamos los comandos drop y create
    with engine.connect() as connection:
        with connection.begin():
            for comando in comandos_drop:
                connection.execute(comando)

            for comando in comandos_create:
                connection.execute(comando)
