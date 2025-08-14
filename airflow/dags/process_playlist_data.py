from airflow.sdk import dag, task
from airflow.providers.amazon.aws.operators.s3 import S3Hook

import pandas as pd
import json
import datetime

@dag
def process_playlist_data():
    
    @task
    def get_playlist_json_data_from_s3(ds):
        conn = S3Hook(aws_conn_id='AWS_CONN')
        s3_client = conn.get_conn()

        s3_response = s3_client.get_object(
            Bucket = 'spotify-api-project-bucket',
            Key = f'json_files/test_{ds}.json'
        )

        s3_object_body = s3_response.get('Body')
        playlist_json_data_bytes = s3_object_body.read().decode('utf-8')
        playlist_json_data = json.loads(playlist_json_data_bytes)
        
        return playlist_json_data
    
    @task
    def dim_playlists(playlist_json_data, ds):
        dim_playlists = {
            'id': playlist_json_data['id'],
            'name': playlist_json_data['name'],
            'description': playlist_json_data['description'],
            'owner_id': playlist_json_data['owner']['id'],
            'total_tracks': playlist_json_data['tracks']['total'],
            'updated_at': ds
        }
        
        print(dim_playlists)
    
    @task
    def dim_tracks(playlist_json_data, ds):

        playlist_tracks = []
        for item in playlist_json_data['tracks']['items']:
            track_data_row = {
                'track_id': item['track']['id'],
                'track_name': item['track']['name'],
                'track_album_id': item['track']['album']['id'],
                'track_album_name': item['track']['album']['name'],
                'track_artist_id': item['track']['album']['artists'][0]['id'],
                'track_artist_name': item['track']['album']['artists'][0]['name'],
                'track_additional_artists': {},
                'playlist_id': playlist_json_data['id'],
                'playlist_name': playlist_json_data['name'],
                'track_added_to_playlist_at': item['added_at']
            }

            # For tracks with multiple artists, store additional artist data together in a group.
            for i in range(1, len(item['track']['album']['artists'])):
                track_data_row['track_additional_artists'].update({
                    i: {
                        'track_artist_id': item['track']['album']['artists'][i]['id'],
                        'track_artist_name': item['track']['album']['artists'][i]['name']
                    }
                })
            
            playlist_tracks.append(track_data_row)
        
        print(playlist_tracks)

    playlist_json_data = get_playlist_json_data_from_s3()
    dim_playlists(playlist_json_data)  
    dim_tracks(playlist_json_data)

process_playlist_data()