from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

@dag
def run_dbt_project():

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="cd /opt/airflow/dbt/spotify_dbt_project/ && dbt deps"
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command="cd /opt/airflow/dbt/spotify_dbt_project/ && dbt seed"
            # "cd /home/pmartin/projects/spotify_api_project/dbt/spotify_dbt_project/ \
            # && eval $(poetry env activate) \
            # && dbt seed"
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt/spotify_dbt_project/ && dbt run"
            # "cd /home/pmartin/projects/spotify_api_project/dbt/spotify_dbt_project/; \
            # eval $(poetry env activate); \
            # dbt run"
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt/spotify_dbt_project/ && dbt test"
            # "cd /home/pmartin/projects/spotify_api_project/dbt/spotify_dbt_project/ \
            # && eval $(poetry env activate) \
            # && dbt test"
    )

    dbt_deps >> dbt_seed >> dbt_run >> dbt_test

run_dbt_project()