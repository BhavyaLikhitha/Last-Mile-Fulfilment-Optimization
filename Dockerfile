# # Dockerfile
# # Custom Airflow 3.1.6 image with all fulfillment platform dependencies pre-installed
# # Baking dependencies in at build time means:
# #   - Containers start in seconds (not minutes)
# #   - No _PIP_ADDITIONAL_REQUIREMENTS needed
# #   - dbt is in PATH
# #   - No laptop hanging

# FROM apache/airflow:3.1.6

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
#     joblib \
#     scipy \
#     pyarrow


FROM apache/airflow:3.1.6

# Install system dependencies as root (lightgbm needs libgomp)
USER root
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 && rm -rf /var/lib/apt/lists/*
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
    pyarrow