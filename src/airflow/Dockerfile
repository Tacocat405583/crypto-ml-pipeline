FROM apache/airflow:2.10.5

USER root
RUN mkdir -p /app/models

USER airflow

COPY requirements.txt /tmp/requirements.txt

RUN pip install -r /tmp/requirements.txt

#for safety
USER airflow 