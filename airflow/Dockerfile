FROM apache/airflow:3.0.4

# Copy dbt project into image
COPY /dbt /opt/airflow/dbt

# Install necessary python packages
RUN pip install \
  pandas==2.2.1 \
  requests==2.32.4 \
  dbt===1.0.0.40.5 \
  dbt-core==1.10.9 \
  dbt-postgres==1.9.0