# DE-proyecto-final
### Desarrollado por Juan Orza

## Requerimientos
- Tener instalado Docker

## Instrucciones
0. El documento `.\dags\project_files\ciudades.csv` contiene el listado de ciudades para las que se va a estar descargando los datos de clima, junto a los valores extremos para el sistema de alertas. Se pueden modificar estos valores antes de levantar el proyecto.
1. `make build`
2. `make run`
3. Ingresar en `localhost:8080` desde cualquier navegador
4. Loguearse con usuario/contraseña: airflow/airflow
5. Ejecutar el DAG `start_database_dag` para llevar a cero el datawarehouse en Amazon Redshift
6. Ejecutar el DAG `main_dag`, que ejecuta con `catchup=True` la descarga de datos del clima para las ciudades configuradas.
A modo de ejemplo, el DAG está programado para descargar valores diarios desde el 2023-03-01 hasta 2023-04-30