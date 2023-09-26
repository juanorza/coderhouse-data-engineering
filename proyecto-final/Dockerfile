FROM apache/airflow:2.3.3-python3.8

ENV AIRFLOW_HOME=/opt/airflow

ADD webserver_config.py /opt/airflow/webserver_config.py

USER airflow

RUN python -m pip install --upgrade pip
RUN pip install psycopg2-binary
RUN pip install pandas==2.0.3
RUN pip install SQLAlchemy