FROM apache/airflow:2.10.5-python3.11
COPY requirements.txt /requirements.txt
ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.11
RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    -r /requirements.txt
