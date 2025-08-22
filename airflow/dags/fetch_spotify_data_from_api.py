from airflow.sdk import dag, task, Variable
from airflow.providers.amazon.aws.operators.s3 import S3Hook
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

import json
import base64
import requests
import logging
import sys
from math import ceil

# Create logger object scoped to this dag file.
logger = logging.getLogger(__name__)
stdout = logging.StreamHandler(stream=sys.stdout)
format = logging.Formatter(
    "%(name)s: %(levelname)s | %(filename)s:%(lineno)s | %(process)d >>> %(message)s"
)
stdout.setFormatter(format)
logger.addHandler(stdout)
logger.setLevel(logging.INFO)

# Set playlist data to import (one of my personal public playlists).
spotify_playlist_ids = {
    'wincing the night away': '1r29zYJJ8SGyx3ZOUm2yTu'
}

# Function to check API endpoint availability.
def check_api_availability(response):
    if response.status_code == 200:
        logger.info('API request successful')
    else:
        logger.warning('API response status code: ' + str(response.status_code))
        if response.status_code == 429:
            logger.info('Retry after ' + str(response.headers.get('Retry-After')) + ' seconds')

# Function to save dictionary to S3 as JSON file.
def save_json_file_to_s3(dict, name, ds):
    
    # Connection created in Airflow UI.
    s3_hook = S3Hook(aws_conn_id='AWS_CONN')

    # Format dictionary to use double-quotes for strings, as required by JSON standards.
    content = json.dumps(dict)

    s3_client = s3_hook.get_conn()
    s3_client.put_object(
        Body=content, 
        Bucket="spotify-api-project-bucket", 
        Key=f"json_files/{name}_json_data_{ds}.json"
    )

@dag
def fetch_spotify_data_from_api():

    @task
    def import_airflow_variables():
        
        # Variables saved in Airflow UI for security purposes (more secure method possible?).
        airflow_variables = {
            "CLIENT_ID": Variable.get('CLIENT_ID'),
            "CLIENT_SECRET": Variable.get('CLIENT_SECRET'), 
        }
        
        return airflow_variables

    @task
    def get_token(airflow_variables):
        
        # The Spotify API requires a base-64 authentication string to receive token.
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

        # Check if the API is available; if not, return error code and possibly other info.
        check_api_availability(response)
        
        playlist_dict = json.loads(response.content)

        # The Spotify API limits playlist data requests to 100 tracks per request by default (but 
        # can be set even lower). For playlists with more tracks than the set limit, multiple 
        # requests are required to get the complete track data.
        request_limit = playlist_dict['tracks']['limit']
        total_tracks = playlist_dict['tracks']['total']
        num_additional_requests = ceil(total_tracks / request_limit) - 1
        next_url = playlist_dict['tracks']['next']

        for i in range(0, num_additional_requests):
            next_response = requests.get(next_url, headers=headers)
            
            # Check if the API is available; if not, return error code and possibly other info.
            check_api_availability(response)
            
            additional_tracks = json.loads(next_response.content)
            playlist_dict['tracks']['items'] += additional_tracks['items']
            next_url = additional_tracks['next']
        
        # Remove values that don't make sense after combining track data.
        playlist_dict['tracks']['next'] = None
        playlist_dict['tracks']['offset'] = None

        # Add key to record total number of requests.
        playlist_dict['tracks']['total_number_of_requests'] = num_additional_requests + 1

        return playlist_dict

    @task.sensor
    def get_playlist_artist_data(playlist_dict, token):
        
        playlist_artist_id_list = []
        for item in playlist_dict['tracks']['items']:
            for i in range(0, len(item['track']['album']['artists'])):
                if item['track']['album']['artists'][i]['id'] not in playlist_artist_id_list:
                    playlist_artist_id_list.append(item['track']['album']['artists'][i]['id'])
        
        # The Spotify API allows you to request data for up to 50 artists in a single batch request.
        # To collect data for more than 50 artists, multiple requests are required.
        total_artist_count = len(playlist_artist_id_list)
        num_requests = ceil(total_artist_count / 50)
        playlist_artist_dict = {'artists': []}

        for i in range(0, num_requests):
            track_artist_id_request_list = playlist_artist_id_list[50*i: min(50*(i+1), total_artist_count)]

            url = 'https://api.spotify.com/v1/artists?ids=' + ",".join(track_artist_id_request_list)
            headers = {
                "Authorization": "Bearer " + token
            }
            
            response = requests.get(url, headers=headers)

            # Check if the API is available; if not, return error code and possibly other info.
            check_api_availability(response)

            additional_artists = json.loads(response.content)
            playlist_artist_dict['artists'] += additional_artists['artists']

        # Add key to record total number of requests.
        playlist_artist_dict['total_number_of_requests'] = num_requests

        return playlist_artist_dict

    # Save JSON data to S3. There are two files to save, so create two similar tasks that both use the
    # "save_json_file_to_s3" function.
    @task
    def save_playlist_json_to_s3(playlist_dict, ds):
        save_json_file_to_s3(playlist_dict, 'playlist', ds)

    @task
    def save_playlist_artist_json_to_s3(playlist_artist_dict, ds):
        save_json_file_to_s3(playlist_artist_dict, 'playlist_artist', ds)

    # Create DAG dependency
    trigger_downstream_dag = TriggerDagRunOperator(
        task_id='trigger_downstream_dag',
        trigger_dag_id='process_spotify_data'
    )

    # Task order
    airflow_variables = import_airflow_variables()
    token = get_token(airflow_variables)

    playlist_dict = get_playlist_data(spotify_playlist_ids['wincing the night away'], token)
    playlist_artist_dict = get_playlist_artist_data(playlist_dict, token)

    [
        save_playlist_json_to_s3(playlist_dict),
        save_playlist_artist_json_to_s3(playlist_artist_dict)
    ] >> trigger_downstream_dag

fetch_spotify_data_from_api()