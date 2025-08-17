from airflow.sdk import dag, task
from airflow.providers.amazon.aws.operators.s3 import S3Hook

import pandas as pd
import json
import io

@dag
def load_spotify_data_into_database():
    
    # Get parquet files from S3. There are 7 files to fetch, so create 7 similar tasks that all use the
    # following "get_parquet_file_from_s3" function.
    def get_parquet_file_from_s3(name, ds):
        
        # Connection created in Airflow UI.
        conn = S3Hook(aws_conn_id='AWS_CONN')
        s3_client = conn.get_conn()

        s3_response = s3_client.get_object(
            Bucket = 'spotify-api-project-bucket',
            #Key = f'json_files/test_{ds}.json'
            Key = f'parquet_files/{name}_{ds}.parquet'
        )

        s3_object_body = s3_response.get('Body')
        json_data_bytes = s3_object_body.read().decode('utf-8')
        json_data = json.loads(json_data_bytes)
        
        return json_data

    @task
    def get_playlist_json_from_s3(ds):
        return get_json_file_from_s3('playlist', ds)