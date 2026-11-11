FROM apache/airflow:3.1.8

USER airflow

RUN python -m pip install --no-cache-dir \
    databricks-sdk==0.117.0 \
    dbt-core==1.12.5 \
    dbt-databricks==1.12.6 \
    chardet==5.2.0