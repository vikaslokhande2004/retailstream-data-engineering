FROM apache/airflow:2.10.5

USER root

RUN apt-get update \
    && apt-get install -y git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow==2.10.5" \
    "apache-airflow-providers-docker==3.13.0"

RUN python -m venv /opt/airflow/dbt-venv \
    && unset PIP_CONSTRAINT \
    && /opt/airflow/dbt-venv/bin/pip install --no-cache-dir \
        dbt-core==1.12.5 \
        dbt-postgres==1.11.0