from airflow.sdk import dag, task
from airflow.providers.amazon.aws.operators.s3 import S3Hook
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

import pandas as pd
import json
import io

# Function to fetch JSON file from S3.
def get_json_file_from_s3(name, ds):
    
    # Connection created in Airflow UI.
    s3_hook = S3Hook(aws_conn_id='AWS_CONN')
    
    s3_client = s3_hook.get_conn()
    s3_response = s3_client.get_object(
        Bucket='spotify-api-project-bucket',
        # Key=f'json_files/{name}_json_data_{ds}.json'
        Key=f'json_files/{name}_json_data_2025-08-18.json'
    )

    s3_object_body = s3_response.get('Body')
    json_data_bytes = s3_object_body.read().decode('utf-8')
    json_data = json.loads(json_data_bytes)
    
    return json_data

# Function to save dataframe to S3 as CSV file (would prefer parquet files, but Amazon has
# no free RDS options that support streamlined parquet ingestion).
def save_csv_file_to_s3(df, df_name, ds):

    # Connection created in Airflow UI.
    s3_hook = S3Hook(aws_conn_id='AWS_CONN')
    
    # Rather than save file locally (to transfer to S3), save file data to "file-like" object.
    f = io.BytesIO()
    df.to_csv(f, index=False)
    f.seek(0)
    content = f.read()

    s3_client = s3_hook.get_conn()
    s3_client.put_object(
        Body=content, 
        Bucket="spotify-api-project-bucket", 
        # Key=f"csv_files/{df_name}_{ds}.csv"
        Key=f"csv_files/{df_name}_2025-08-18.csv"
    )

@dag
def process_spotify_data():
    
    # Get JSON files from S3. There are two files to fetch, so create two similar tasks that both use the
    # "get_json_file_from_s3" function.
    @task
    def get_playlist_json_from_s3(ds):
        return get_json_file_from_s3('playlist', ds)

    @task
    def get_playlist_artist_json_from_s3(ds):
        return get_json_file_from_s3('playlist_artist', ds)
    
    @task
    def create_dim_playlists_csv(playlist_json_data, ts, ds):
        
        dim_playlists = [{
            'playlist_id': playlist_json_data['id'],
            'playlist_name': playlist_json_data['name'],
            'playlist_description': playlist_json_data['description'],
            'playlist_owner_id': playlist_json_data['owner']['id'],
            'playlist_total_tracks': playlist_json_data['tracks']['total'],
            'playlist_total_followers': playlist_json_data['followers']['total'],
            'playlist_external_url': playlist_json_data['external_urls']['spotify'],
            'last_updated_at': ts
        }]
        
        df = pd.DataFrame(dim_playlists)
        save_csv_file_to_s3(df, 'dim_playlists', ds)
    
    @task
    def create_fact_playlist_popularity_and_size_csv(playlist_json_data, ts, ds):
        
        fact_playlist_popularity_and_size = [{
            'playlist_id': playlist_json_data['id'],
            'date': ds,
            'playlist_total_tracks': playlist_json_data['tracks']['total'],
            'playlist_total_followers': playlist_json_data['followers']['total'],
            'last_updated_at': ts
        }]
        
        df = pd.DataFrame(fact_playlist_popularity_and_size)
        save_csv_file_to_s3(df, 'fact_playlist_popularity_and_size', ds)
    
    @task
    def create_dim_tracks_csv(playlist_json_data, ts, ds):

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
                'track_external_url': item['track']['external_urls']['spotify'],
                'last_updated_at': ts
            }

            # For tracks with multiple artists, store additional artist data together in a dictionary.
            for i in range(1, len(item['track']['album']['artists'])):
                track_data_row['track_additional_artists'].update({
                    str(i + 1): {
                        'track_artist_id': item['track']['album']['artists'][i]['id'],
                        'track_artist_name': item['track']['album']['artists'][i]['name']
                    }
                })

            # Format dictionary to use double-quotes for strings, as required by JSON standards.
            track_data_row['track_additional_artists'] = json.dumps(track_data_row['track_additional_artists'])
            
            playlist_tracks_list.append(track_data_row)
        
        
        df = pd.DataFrame(playlist_tracks_list)
        save_csv_file_to_s3(df, 'dim_tracks', ds)

    @task
    def create_dim_track_artists_csv(playlist_json_data, ts, ds):

        track_artists_list = []
        for item in playlist_json_data['tracks']['items']:
            for i in range(0, len(item['track']['album']['artists'])):
                track_artist_data_row = {
                    'track_id': item['track']['id'],
                    'track_name': item['track']['name'],
                    'track_artist_id': item['track']['album']['artists'][i]['id'],
                    'track_artist_name': item['track']['album']['artists'][i]['name'],
                    'track_artist_order': i + 1,
                    'last_updated_at': ts
                }

                track_artists_list.append(track_artist_data_row)
        
        df = pd.DataFrame(track_artists_list)
        save_csv_file_to_s3(df, 'dim_track_artists', ds)
    
    @task
    def create_fact_track_popularity_csv(playlist_json_data, ts, ds):
        
        track_popularity_list = []
        for item in playlist_json_data['tracks']['items']:
            track_popularity_data_row = {
                'track_id': item['track']['id'],
                'date': ds,
                'track_popularity': item['track']['popularity'],
                'last_updated_at': ts
            }

            track_popularity_list.append(track_popularity_data_row)
        
        df = pd.DataFrame(track_popularity_list)
        save_csv_file_to_s3(df, 'fact_track_popularity', ds)

    @task
    def create_dim_artists_csv(playlist_artist_json_data, ts, ds):
        
        artists_list = []
        for item in playlist_artist_json_data['artists']:
            artist_data_row = {
                'artist_id': item['id'],
                'artist_name': item['name'],
                'artist_genres': {},
                'artist_popularity': item['popularity'],
                'artist_total_followers': item['followers']['total'],
                'artist_external_url': item['external_urls']['spotify'],
                'last_updated_at': ts
            }

            # For artists with multiple genres, store additional genre data together in a dictionary.
            for i in range(0, len(item['genres'])):
                artist_data_row['artist_genres'].update({
                    str(i + 1): item['genres'][i]
                })

            # Format dictionary to use double-quotes for strings, as required by JSON standards.
            artist_data_row['artist_genres'] = json.dumps(artist_data_row['artist_genres'])

            artists_list.append(artist_data_row)
        
        df = pd.DataFrame(artists_list)
        save_csv_file_to_s3(df, 'dim_artists', ds)
    
    @task
    def create_dim_artist_genres_csv(playlist_artist_json_data, ts, ds):

        artist_genres_list = []
        for item in playlist_artist_json_data['artists']:
            for i in range(0, len(item['genres'])):
                artist_genre_data_row = {
                    'artist_id': item['id'],
                    'artist_name': item['name'],
                    'artist_genre': item['genres'][i],
                    'artist_genre_order': i + 1,
                    'last_updated_at': ts
                }
            
                artist_genres_list.append(artist_genre_data_row)
        
        df = pd.DataFrame(artist_genres_list)
        save_csv_file_to_s3(df, 'dim_artist_genres', ds)
    
    @task
    def create_fact_artist_popularity_csv(playlist_artist_json_data, ts, ds):
        
        artist_popularity_list = []
        for item in playlist_artist_json_data['artists']:
            artist_popularity_data_row = {
                'artist_id': item['id'],
                'date': ds,
                'artist_popularity': item['popularity'],
                'artist_total_followers': item['followers']['total'],
                'last_updated_at': ts
            }

            artist_popularity_list.append(artist_popularity_data_row)
        
        df = pd.DataFrame(artist_popularity_list)
        save_csv_file_to_s3(df, 'fact_artist_popularity', ds)
    
    # Create DAG dependency
    trigger_downstream_dag = TriggerDagRunOperator(
        task_id='trigger_downstream_dag',
        trigger_dag_id='load_spotify_data_into_database'
    )
    
    # Task order
    playlist_json_data = get_playlist_json_from_s3()
    playlist_artist_json_data = get_playlist_artist_json_from_s3()
    [
        create_dim_playlists_csv(playlist_json_data),
        create_fact_playlist_popularity_and_size_csv(playlist_json_data),
        create_dim_tracks_csv(playlist_json_data),
        create_dim_track_artists_csv(playlist_json_data),
        create_fact_track_popularity_csv(playlist_json_data),

        create_dim_artists_csv(playlist_artist_json_data),
        create_dim_artist_genres_csv(playlist_artist_json_data),
        create_fact_artist_popularity_csv(playlist_artist_json_data)
    ] >> trigger_downstream_dag

process_spotify_data()