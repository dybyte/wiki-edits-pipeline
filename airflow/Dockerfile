FROM apache/airflow:3.4.0-python3.12

# Airflow와 dbt는 의존성 범위가 다르므로 dbt를 별도 venv에 격리한다.
USER root
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir \
        dbt-core==1.12.6 \
        dbt-clickhouse==1.10.3
USER airflow
