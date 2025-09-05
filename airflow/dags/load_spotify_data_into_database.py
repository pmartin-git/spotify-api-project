from airflow.sdk import dag, task
from airflow.providers.amazon.aws.operators.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

import pandas as pd

# Function to fetch CSV file from S3 and copy into Amazon RDS Postgres cloud database.
def load_csv_and_copy_into_postgres(table_name, target_fields, replace_index, ds):
    
    # Connections created in Airflow UI.
    s3_hook = S3Hook(aws_conn_id='AWS_CONN')
    pg_hook = PostgresHook(postgres_conn_id='amazon_rds_postgres')

    s3_client = s3_hook.get_conn()
    s3_response = s3_client.get_object(
        Bucket='spotify-api-project-bucket',
        Key=f'csv_files/{table_name}_{ds}.csv'
    )
    
    s3_object_body = s3_response.get('Body')
    df = pd.read_csv(s3_object_body)
    data_to_insert = [tuple(row) for row in df.values]
    pg_hook.insert_rows(
        table=f'spotify_sources.{table_name}',
        rows=data_to_insert,
        target_fields=target_fields,
        replace=True,
        replace_index=replace_index,
        executemany=1000
    )

@dag
def load_spotify_data_into_database():

    @task
    def copy_dim_playlists_into_postgres(ds):

        target_fields = [
            'playlist_id',
            'playlist_name',
            'playlist_description',
            'playlist_owner_id',
            'playlist_total_tracks',
            'playlist_total_followers',
            'playlist_external_url',
            'last_updated_at'
        ]
        replace_index=['playlist_id']
        load_csv_and_copy_into_postgres('dim_playlists', target_fields, replace_index, ds)

    @task
    def copy_fact_playlist_popularity_and_size_into_postgres(ds):

        target_fields = [
            'playlist_id',
            'date',
            'playlist_total_tracks',
            'playlist_total_followers',
            'last_updated_at'
        ]
        replace_index=[
            'playlist_id',
            'date'
        ]
        load_csv_and_copy_into_postgres('fact_playlist_popularity_and_size', target_fields, replace_index, ds)

    @task
    def copy_dim_tracks_into_postgres(ds):

        target_fields = [
            'track_id',
            'track_name',
            'track_album_id',
            'track_album_name',
            'track_artist_id',
            'track_artist_name',
            'track_additional_artists',
            'playlist_id',
            'playlist_name',
            'track_added_to_playlist_at',
            'track_popularity',
            'track_external_url',
            'last_updated_at'
        ]
        replace_index=['track_id']
        load_csv_and_copy_into_postgres('dim_tracks', target_fields, replace_index, ds)
    
    @task
    def copy_dim_track_artists_into_postgres(ds):

        target_fields = [
            'track_id',
            'track_name',
            'track_artist_id',
            'track_artist_name',
            'track_artist_order',
            'last_updated_at'
        ]
        replace_index=[
            'track_id',
            'track_artist_id'
        ]
        load_csv_and_copy_into_postgres('dim_track_artists', target_fields, replace_index, ds)
    
    @task
    def copy_fact_track_popularity_into_postgres(ds):

        target_fields = [
            'track_id',
            'date',
            'track_popularity',
            'last_updated_at'
        ]
        replace_index=[
            'track_id',
            'date'
        ]
        load_csv_and_copy_into_postgres('fact_track_popularity', target_fields, replace_index, ds)
    
    @task
    def copy_dim_artists_into_postgres(ds):

        target_fields = [
            'artist_id',
            'artist_name',
            'artist_genres',
            'artist_popularity',
            'artist_total_followers',
            'artist_external_url',
            'last_updated_at'
        ]
        replace_index=['artist_id']
        load_csv_and_copy_into_postgres('dim_artists', target_fields, replace_index, ds)
    
    @task
    def copy_dim_artist_genres_into_postgres(ds):

        target_fields = [
            'artist_id',
            'artist_name',
            'artist_genre',
            'artist_genre_order',
            'last_updated_at'
        ]
        replace_index=[
            'artist_id',
            'artist_genre'
        ]
        load_csv_and_copy_into_postgres('dim_artist_genres', target_fields, replace_index, ds)
    
    @task
    def copy_fact_artist_popularity_into_postgres(ds):

        target_fields = [
            'artist_id',
            'date',
            'artist_popularity',
            'artist_total_followers',
            'last_updated_at'
        ]
        replace_index=[
            'artist_id',
            'date'
        ]
        load_csv_and_copy_into_postgres('fact_artist_popularity', target_fields, replace_index, ds)

    # Create DAG dependency
    trigger_downstream_dag = TriggerDagRunOperator(
        task_id='trigger_downstream_dag',
        trigger_dag_id='run_dbt_project'
    )

    # Task order
    [
        copy_dim_playlists_into_postgres(),
        copy_fact_playlist_popularity_and_size_into_postgres(),
        copy_dim_tracks_into_postgres(),
        copy_dim_track_artists_into_postgres(),
        copy_fact_track_popularity_into_postgres(),
        copy_dim_artists_into_postgres(),
        copy_dim_artist_genres_into_postgres(),
        copy_fact_artist_popularity_into_postgres()
    ] >> trigger_downstream_dag

load_spotify_data_into_database()