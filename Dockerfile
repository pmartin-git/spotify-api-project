FROM apache/airflow:3.0.4

# Copy dbt project into image
COPY airflow/dbt /opt/airflow/dbt

# Copy Poetry files into image
COPY pyproject.toml .
COPY poetry.lock .

# Install Poetry
RUN pip install poetry==2.1.4

# Use Poetry to install Python packages as specified in poetry.lock file
RUN poetry config virtualenvs.create false --local \
    && poetry install --no-interaction --no-ansi