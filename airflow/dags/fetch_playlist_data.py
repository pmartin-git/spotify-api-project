from airflow.sdk import dag, task, Variable
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator

import json
import base64
import requests
import datetime

spotify_playlist_ids = {
    'wincing the night away': '1r29zYJJ8SGyx3ZOUm2yTu'
}

@dag(
    start_date=datetime.datetime(2025, 8, 1),
    schedule='0 6 * * *'
)
def fetch_playlist_data():

    @task
    def import_airflow_variables():
        airflow_variables = {
            "CLIENT_ID": Variable.get('CLIENT_ID'),
            "CLIENT_SECRET": Variable.get('CLIENT_SECRET'), 
        }
        
        return airflow_variables

    @task
    def get_token(airflow_variables):
        auth_string = airflow_variables["CLIENT_ID"] + ":" + airflow_variables["CLIENT_SECRET"]
        auth_bytes = auth_string.encode('utf-8')
        auth_base64 = str(base64.b64encode(auth_bytes), 'utf-8')
        
        url = 'https://accounts.spotify.com/api/token'
        headers = {
            'Authorization': 'Basic ' + auth_base64,
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        data = {
            'grant_type': 'client_credentials'
        }

        response = requests.post(url, headers=headers, data=data)
        response_json = json.loads(response.content)
        token = response_json['access_token']

        return token

    @task
    def get_playlist_data(playlist_id, token):

        url = 'https://api.spotify.com/v1/playlists/' + playlist_id
        headers = {
            "Authorization": "Bearer " + token
        }
        
        response = requests.get(url, headers=headers)
        playlist_dict = json.loads(response.content)

        # Format dictionary data to use double-quotes for strings, as required by JSON standards.
        playlist_json_data = json.dumps(playlist_dict)

        return playlist_json_data

    save_playlist_json_to_s3 = S3CreateObjectOperator(
        task_id = 'save_playlist_json_to_s3',
        aws_conn_id = 'AWS_CONN',
        s3_bucket = 'spotify-api-project-bucket',
        s3_key = 'json_files/test_' + '{{ ds }}' + '.json',
        data = '{{ ti.xcom_pull(task_ids="get_playlist_data") }}'
    )

    airflow_variables = import_airflow_variables()
    token = get_token(airflow_variables)
    get_playlist_data(spotify_playlist_ids['wincing the night away'], token) >> save_playlist_json_to_s3

fetch_playlist_data()