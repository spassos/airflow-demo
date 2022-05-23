FROM apache/airflow:2.3.0

USER root

ARG AIRFLOW_HOME=/opt/airflow
ADD dags /opt/airflow/dags

USER airflow

USER ${AIRFLOW_UID}

COPY requirements.txt .

RUN pip install --trusted-host pypi.python.org -r requirements.txt
