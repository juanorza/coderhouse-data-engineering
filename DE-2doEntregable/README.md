# DE-2doEntregable

El script `main.py` ejecuta el proceso completo de ETL. Invoca las funciones principales de los módulos `extract.py`, `transform.py` y `load.py` para la ejecución de cada instancia.

Para que el proceso no demore demasiado, en esta resolución limitamos la descarga de información del clima a una ventana concreta de tiempo (enero 2023) y solo obtendremos valores diarios para cada ciudad. 

El script `start_db.py` se conecta a la base de datos de Amazon Redshift provista para este proyecto y crea la tabla incial para almacenar la información descargada en el script anterior.

![image](https://github.com/juanorza/coderhouse-data-engineering/assets/68288013/dc65e0ed-303e-46c3-af91-6514df249735)
