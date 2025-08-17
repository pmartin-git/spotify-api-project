from airflow.sdk import dag, task, Variable
from airflow.providers.amazon.aws.operators.s3 import S3Hook

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
        playlist_dict = json.loads(response.content)

        # The Spotify API limits playlist data requests to 100 tracks per request by default (but 
        # can be set to even fewer). For playlists with more tracks than the set limit, multiple 
        # requests are required to get the complete track data.
        request_limit = playlist_dict['tracks']['limit']
        total_tracks = playlist_dict['tracks']['total']
        num_additional_requests = int(round(total_tracks / request_limit, 0))
        next_url = playlist_dict['tracks']['next']

        for i in range(0, num_additional_requests):
            next_response = requests.get(next_url, headers=headers)
            additional_tracks = json.loads(next_response.content)
            playlist_dict['tracks']['items'] += additional_tracks['items']
            next_url = additional_tracks['next']
        
        # Set values for final dictionary that make sense after combining track data.
        playlist_dict['tracks']['next'] = None
        playlist_dict['tracks']['offset'] = None

        # Add a key to record total number of requests.
        playlist_dict['tracks']['total_number_of_requests'] = num_additional_requests + 1

        return playlist_dict

    @task
    def get_playlist_artist_data(playlist_dict, token):
        
        playlist_artist_id_list = []
        for item in playlist_dict['tracks']['items']:
            for i in range(0, len(item['track']['album']['artists'])):
                playlist_artist_id_list.append(item['track']['album']['artists'][i]['id'])
        
        # The Spotify API allows you to request data for up to 50 artists in a single request.
        # To collect data for more than 50 artists, multiple requests are required.
        total_artist_count = len(playlist_artist_id_list)
        num_requests = int(round(total_artist_count / 50, 0)) + 1
        playlist_artist_dict = {"artists": []}

        for i in range(0, num_requests):
            track_artist_id_request_list = playlist_artist_id_list[50*i: min(50*(i+1), total_artist_count)]

            url = 'https://api.spotify.com/v1/artists?ids=' + ",".join(track_artist_id_request_list)
            headers = {
                "Authorization": "Bearer " + token
            }
            
            response = requests.get(url, headers=headers)
            additional_artists = json.loads(response.content)
            playlist_artist_dict["artists"] += additional_artists["artists"]

        return playlist_artist_dict

    @task
    def save_playlist_json_to_s3(playlist_dict, ds):
        
        # Format dictionary data to use double-quotes for strings, as required by JSON standards.
        playlist_json_data = json.dumps(playlist_dict)

        conn = S3Hook(aws_conn_id='AWS_CONN')
        s3_client = conn.get_conn()

        s3_client.put_object(
            Body=playlist_json_data,
            Bucket="spotify-api-project-bucket", 
            Key=f"json_files/playlist_json_data_{ds}.json"
        )

    @task
    def save_playlist_artist_json_to_s3(playlist_artist_dict, ds):
        
        # Format dictionary data to use double-quotes for strings, as required by JSON standards.
        playlist_artist_json_data = json.dumps(playlist_artist_dict)

        conn = S3Hook(aws_conn_id='AWS_CONN')
        s3_client = conn.get_conn()
        s3_client.put_object(
            Body=playlist_artist_json_data,
            Bucket="spotify-api-project-bucket", 
            Key=f"json_files/playlist_artist_json_data_{ds}.json"
        )

    airflow_variables = import_airflow_variables()
    token = get_token(airflow_variables)
    playlist_dict = get_playlist_data(spotify_playlist_ids['wincing the night away'], token)
    playlist_artist_dict = get_playlist_artist_data(playlist_dict, token)
    save_playlist_json_to_s3(playlist_dict)
    save_playlist_artist_json_to_s3(playlist_artist_dict)

fetch_spotify_data_from_api()