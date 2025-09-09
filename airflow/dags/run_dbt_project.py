from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator

@dag
def run_dbt_project():

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="cd /opt/airflow/dbt/spotify_dbt_project/ && dbt deps"
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command="cd /opt/airflow/dbt/spotify_dbt_project/ && dbt seed"
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt/spotify_dbt_project/ && dbt run"
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt/spotify_dbt_project/ && dbt test"
    )

    # Task order
    dbt_deps >> dbt_seed >> dbt_run >> dbt_test

run_dbt_project()