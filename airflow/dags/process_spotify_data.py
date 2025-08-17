from airflow.sdk import dag, task
from airflow.providers.amazon.aws.operators.s3 import S3Hook

import pandas as pd
import json
import io

@dag
def process_spotify_data():
    
    # Get JSON files from S3. There are two files to fetch, so create two similar tasks that both use the
    # following "get_json_file_from_s3" function.
    def get_json_file_from_s3(name, ds):
        
        # Connection created in Airflow UI (more secure method possible?).
        conn = S3Hook(aws_conn_id='AWS_CONN')
        s3_client = conn.get_conn()

        s3_response = s3_client.get_object(
            Bucket = 'spotify-api-project-bucket',
            #Key = f'json_files/test_{ds}.json'
            Key = f'json_files/{name}_json_data_{ds}.json'
        )

        s3_object_body = s3_response.get('Body')
        json_data_bytes = s3_object_body.read().decode('utf-8')
        json_data = json.loads(json_data_bytes)
        
        return json_data

    @task
    def get_playlist_json_from_s3(ds):
        return get_json_file_from_s3('playlist', ds)

    @task
    def get_playlist_artist_json_from_s3(ds):
        return get_json_file_from_s3('playlist_artist', ds)
    
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

            # For tracks with multiple artists, store additional artist data together in a dictionary.
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
    def create_fact_track_popularity(playlist_json_data, ds):
        
        track_popularity_list = []
        for item in playlist_json_data['tracks']['items']:
            track_popularity_data_row = {
                'track_id': item['track']['id'],
                'date': ds,
                'track_popularity': item['track']['popularity']
            }

            track_popularity_list.append(track_popularity_data_row)
        
        df = pd.DataFrame(track_popularity_list)
        return df

    @task
    def create_dim_artists(playlist_artist_json_data, ds):
        
        artists_list = []
        for item in playlist_artist_json_data['artists']:
            artist_data_row = {
                'artist_id': item['id'],
                'artist_name': item['name'],
                'artist_genres': {},
                'artist_spotify_url': item['external_urls']['spotify']
            }

            # For artists with multiple genres, store additional genre data together in a dictionary.
            for i in range(0, len(item['genres'])):
                artist_data_row['artist_genres'].update({
                    str(i + 1): item['genres'][i]
                })

            artists_list.append(artist_data_row)
        
        df = pd.DataFrame(artists_list)
        return df
    
    @task
    def create_dim_artist_genres(playlist_artist_json_data, ds):

        artist_genres_list = []
        for item in playlist_artist_json_data['artists']:
            for i in range(0, len(item['genres'])):
                artist_genre_data_row = {
                    'artist_id': item['id'],
                    'artist_name': item['name'],
                    'artist_genre': item['genres'][i]
                }
            
            artist_genres_list.append(artist_genre_data_row)
        
        df = pd.DataFrame(artist_genres_list)
        return df
    
    @task
    def create_fact_artist_popularity(playlist_artist_json_data, ds):
        
        artist_popularity_list = []
        for item in playlist_artist_json_data['artists']:
            artist_popularity_data_row = {
                'artist_id': item['id'],
                'date': ds,
                'artist_popularity': item['popularity'],
                'artist_total_followers': item['followers']['total']
            }

            artist_popularity_list.append(artist_popularity_data_row)
        
        df = pd.DataFrame(artist_popularity_list)
        return df
    
    # Save parquet files to S3. There are 7 files to save, so create 7 similar tasks that all use the
    # following "save_parquet_file_to_s3" function.
    def save_parquet_files_to_s3(df, df_name, ds):

        # Connection created in Airflow UI (more secure method possible?).
        conn = S3Hook(aws_conn_id='AWS_CONN')
        s3_client = conn.get_conn()
        
        # Rather than save files locally (to transfer to S3), save file data to data buffer.
        f = io.BytesIO()
        df.to_parquet(f)
        f.seek(0)
        content = f.read()

        s3_client.put_object(
            Body=content, 
            Bucket="spotify-api-project-bucket", 
            Key=f"parquet_files/{df_name}_{ds}.parquet"
        )
    
    @task
    def save_dim_playlists_parquet_to_s3(dim_playlist, ds):
        save_parquet_files_to_s3(dim_playlist, 'dim_playlists', ds)
    
    @task
    def save_dim_tracks_parquet_to_s3(dim_tracks, ds):
        save_parquet_files_to_s3(dim_tracks, 'dim_tracks', ds)

    @task
    def save_dim_track_artists_parquet_to_s3(dim_track_artists, ds):
        save_parquet_files_to_s3(dim_track_artists, 'dim_track_artists', ds)
    
    @task
    def save_fact_track_popularity_parquet_to_s3(fact_track_popularity, ds):
        save_parquet_files_to_s3(fact_track_popularity, 'fact_track_popularity', ds)
    
    @task
    def save_dim_artists_parquet_to_s3(dim_artists, ds):
        save_parquet_files_to_s3(dim_artists, 'dim_artists', ds)
    
    @task
    def save_dim_artist_genres_parquet_to_s3(dim_artist_genres, ds):
        save_parquet_files_to_s3(dim_artist_genres, 'dim_artist_genres', ds)
    
    @task
    def save_fact_artist_popularity_parquet_to_s3(fact_artist_popularity, ds):
        save_parquet_files_to_s3(fact_artist_popularity, 'fact_artist_popularity', ds)

    # Task order
    playlist_json_data = get_playlist_json_from_s3()
    playlist_artist_json_data = get_playlist_artist_json_from_s3()

    dim_playlists = create_dim_playlists(playlist_json_data)  
    dim_tracks = create_dim_tracks(playlist_json_data)
    dim_track_artists = create_dim_track_artists(playlist_json_data)
    fact_track_popularity = create_fact_track_popularity(playlist_json_data)
    dim_artists = create_dim_artists(playlist_artist_json_data)
    dim_artist_genres = create_dim_artist_genres(playlist_artist_json_data)
    fact_artist_popularity = create_fact_artist_popularity(playlist_artist_json_data)

    save_dim_playlists_parquet_to_s3(dim_playlists)
    save_dim_tracks_parquet_to_s3(dim_tracks)
    save_dim_track_artists_parquet_to_s3(dim_track_artists)
    save_fact_track_popularity_parquet_to_s3(fact_track_popularity)
    save_dim_artists_parquet_to_s3(dim_artists)
    save_dim_artist_genres_parquet_to_s3(dim_artist_genres)
    save_fact_artist_popularity_parquet_to_s3(fact_artist_popularity)

process_spotify_data()