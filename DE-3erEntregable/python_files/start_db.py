import config
from sqlalchemy import create_engine

user = config.USER
password = config.PASSWORD
host = config.HOST
port = config.PORT
database = config.DATABASE

db_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
engine = create_engine(db_string, executemany_mode="batch")

# Definimos las tablas para almacenar las mediciones descargadas
table_names = ("mediciones_diarias",)

# Eliminamos las tablas, para hacer un comienzo nuevo
comandos_drop = [f"DROP TABLE IF EXISTS {table_name}" for table_name in table_names]

# Comandos SQL para crear las tablas nuevamente
comandos_create = []

comandos_create.append(
    """
    CREATE TABLE mediciones_diarias (
        city_id VARCHAR(60),
        time DATE,
        weathercode INTEGER,
        temperature_2m_max REAL,
        temperature_2m_min REAL,
        temperature_2m_mean REAL,
        sunrise TIMESTAMP,
        sunset TIMESTAMP,
        constraint pk_city_time
            primary key (city_id, time)
    )
    """
)


def start_db():
    with engine.connect() as connection:
        with connection.begin():
            print(f"Ejecutando comandos DROP sobre las tablas {table_names=}")
            for comando in comandos_drop:
                connection.execute(comando)

            print(f"Ejecutando comandos CREATE sobre las tablas {table_names=}")
            for comando in comandos_create:
                connection.execute(comando)


if __name__ == "__main__":
    # Ejecutamos los comandos drop y create
    start_db()
