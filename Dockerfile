FROM apache/airflow:2.10.5-python3.12

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        vim \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --upgrade pip

RUN pip install --no-cache-dir \
    dbt-core==1.10.15 \
    dbt-bigquery==1.10.3

RUN pip install --no-cache-dir \
    apache-airflow-providers-mongo==5.0.3 \
    pymongo==4.10.1