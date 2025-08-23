from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

DBT_PROJECT_PATH = "/home/pmartin/projects/spotify_api_project/dbt/spotify_dbt_project/"

@dag
def run_dbt_project():

    run_dbt_deps = BashOperator(
        task_id="run_dbt_deps",
        bash_command=
            "cd /home/pmartin/projects/spotify_api_project/dbt/spotify_dbt_project/; \
            eval $(poetry env activate); \
            dbt deps"
    )

    run_dbt_seed = BashOperator(
        task_id="run_dbt_seed",
        bash_command=
            "cd /home/pmartin/projects/spotify_api_project/dbt/spotify_dbt_project/ \
            && eval $(poetry env activate) \
            && dbt seed"
    )

    run_dbt_run = BashOperator(
        task_id="run_dbt_run",
        bash_command=
            "cd /home/pmartin/projects/spotify_api_project/dbt/spotify_dbt_project/; \
            eval $(poetry env activate); \
            dbt run"
    )

    run_dbt_test = BashOperator(
        task_id="run_dbt_test",
        bash_command=
            "cd /home/pmartin/projects/spotify_api_project/dbt/spotify_dbt_project/ \
            && eval $(poetry env activate) \
            && dbt test"
    )

    run_dbt_deps >> run_dbt_seed >> run_dbt_run >> run_dbt_test

run_dbt_project()