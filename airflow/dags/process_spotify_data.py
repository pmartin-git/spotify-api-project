from airflow.sdk import dag, task
from airflow.providers.amazon.aws.operators.s3 import S3Hook

import pandas as pd
import json
import io

@dag
def process_spotify_data():
    
    @task
    def get_playlist_json_from_s3(ds):
        conn = S3Hook(aws_conn_id='AWS_CONN')
        s3_client = conn.get_conn()

        s3_response = s3_client.get_object(
            Bucket = 'spotify-api-project-bucket',
            #Key = f'json_files/test_{ds}.json'
            Key = 'json_files/playlist_json_data_2025-08-16.json'
        )

        s3_object_body = s3_response.get('Body')
        playlist_json_data_bytes = s3_object_body.read().decode('utf-8')
        playlist_json_data = json.loads(playlist_json_data_bytes)
        
        return playlist_json_data
    
    @task
    def get_playlist_artist_json_from_s3(ds):
        conn = S3Hook(aws_conn_id='AWS_CONN')
        s3_client = conn.get_conn()

        s3_response = s3_client.get_object(
            Bucket = 'spotify-api-project-bucket',
            #Key = f'json_files/test_{ds}.json'
            Key = 'json_files/playlist_artist_json_data_2025-08-16.json'
        )

        s3_object_body = s3_response.get('Body')
        playlist_artist_json_data_bytes = s3_object_body.read().decode('utf-8')
        playlist_artist_json_data = json.loads(playlist_artist_json_data_bytes)
        
        return playlist_artist_json_data
    
    @task
    def create_dim_playlists(playlist_json_data, ds):
        dim_playlists = [{
            'id': playlist_json_data['id'],
            'name': playlist_json_data['name'],
            'description': playlist_json_data['description'],
            'owner_id': playlist_json_data['owner']['id'],
            'total_tracks': playlist_json_data['tracks']['total'],
            'updated_at': ds
        }]
        
        df = pd.DataFrame(dim_playlists)
        return df
    
    @task
    def create_dim_tracks(playlist_json_data, ds):

        playlist_tracks_list = []
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
                'track_added_to_playlist_at': item['added_at'],
                'track_popularity': item['track']['popularity'],
                'updated_at': ds
            }

            # For tracks with multiple artists, store additional artist data together in a group.
            for i in range(1, len(item['track']['album']['artists'])):
                track_data_row['track_additional_artists'].update({
                    str(i + 1): {
                        'track_artist_id': item['track']['album']['artists'][i]['id'],
                        'track_artist_name': item['track']['album']['artists'][i]['name']
                    }
                })
            
            playlist_tracks_list.append(track_data_row)
        
        df = pd.DataFrame(playlist_tracks_list)
        return df

    @task
    def create_dim_track_artists(playlist_json_data, ds):

        track_artists_list = []
        for item in playlist_json_data['tracks']['items']:
            for i in range(0, len(item['track']['album']['artists'])):
                track_artist_data_row = {
                    'track_id': item['track']['id'],
                    'track_name': item['track']['name'],
                    'track_artist_id': item['track']['album']['artists'][i]['id'],
                    'track_artist_name': item['track']['album']['artists'][i]['name'],
                    'track_artist_order': i + 1,
                    'updated_at': ds
                }

                track_artists_list.append(track_artist_data_row)
        
        df = pd.DataFrame(track_artists_list)
        return df
    
    @task
    def save_parquet_files_to_s3(dim_playlists, dim_tracks, dim_track_artists, ds):
        conn = S3Hook(aws_conn_id='AWS_CONN')
        s3_client = conn.get_conn()
        
        dataframes_to_write = {
            "dim_playlists": dim_playlists,
            "dim_tracks": dim_tracks,
            "dim_track_artists": dim_track_artists
        }

        for df_name, df in dataframes_to_write.items():
            f = io.BytesIO()
            df.to_parquet(f)
            f.seek(0)
            content = f.read()

            s3_client.put_object(
                Body=content, 
                Bucket="spotify-api-project-bucket", 
                Key=f"parquet_files/{df_name}_{ds}.parquet"
            )
    
    playlist_json_data = get_playlist_json_from_s3()
    playlist_artist_json_data = get_playlist_artist_json_from_s3()
    dim_playlists = create_dim_playlists(playlist_json_data)  
    dim_tracks = create_dim_tracks(playlist_json_data)
    dim_track_artists = create_dim_track_artists(playlist_json_data)
    save_parquet_files_to_s3(dim_playlists, dim_tracks, dim_track_artists)

process_spotify_data()