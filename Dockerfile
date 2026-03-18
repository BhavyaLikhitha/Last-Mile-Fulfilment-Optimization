# ══════════════════════════════════════════════════════════════
# ACTIVE: v1 — Lightweight image (S3-only pipeline)
# To switch to v2 (Kafka + Spark + GX):
#   1. Comment out the v1 section below
#   2. Uncomment the v2 section at the bottom
#   3. Rebuild: docker-compose build
# ══════════════════════════════════════════════════════════════

FROM apache/airflow:3.1.6

# Install system dependencies as root (lightgbm needs libgomp)
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*
USER airflow

# Install all required packages as airflow user
RUN pip install --no-cache-dir \
    apache-airflow-providers-snowflake \
    apache-airflow-providers-amazon \
    apache-airflow-providers-common-sql \
    apache-airflow-providers-standard \
    dbt-snowflake \
    snowflake-connector-python \
    scikit-learn \
    xgboost \
    lightgbm \
    joblib \
    scipy \
    pyarrow \
    pydantic


# ══════════════════════════════════════════════════════════════
# v2: Full image with Kafka, Spark, GX (uncomment for v2)
# Also uncomment Kafka/Spark services in docker-compose.yml
# and switch to v2 DAG in fulfillment_pipeline_dag.py
# ══════════════════════════════════════════════════════════════

# FROM apache/airflow:3.1.6
#
# # Install system dependencies as root (lightgbm needs libgomp, Spark needs JRE)
# USER root
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     libgomp1 \
#     default-jre-headless \
#     && rm -rf /var/lib/apt/lists/*
# USER airflow
#
# # Install all required packages as airflow user
# RUN pip install --no-cache-dir \
#     apache-airflow-providers-snowflake \
#     apache-airflow-providers-amazon \
#     apache-airflow-providers-common-sql \
#     apache-airflow-providers-standard \
#     dbt-snowflake \
#     snowflake-connector-python \
#     scikit-learn \
#     xgboost \
#     lightgbm \
#     joblib \
#     scipy \
#     pyarrow \
#     confluent-kafka \
#     pyspark==3.5.1 \
#     great-expectations[snowflake] \
#     pydantic
