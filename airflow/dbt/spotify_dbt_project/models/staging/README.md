Staging models created using codegen (see packages.yml in root dbt project directory).

$ dbt run-operation generate_base_model --args '{"source_name": "spotify", "table_name": "dim_artists"}' > stg_dim_artists.sql;
dbt run-operation generate_base_model --args '{"source_name": "spotify", "table_name": "dim_artist_genres"}' > stg_dim_artist_genres.sql;
dbt run-operation generate_base_model --args '{"source_name": "spotify", "table_name": "dim_playlists"}' > stg_dim_playlists.sql;
dbt run-operation generate_base_model --args '{"source_name": "spotify", "table_name": "dim_tracks"}' > stg_dim_tracks.sql;
dbt run-operation generate_base_model --args '{"source_name": "spotify", "table_name": "dim_track_artists"}' > stg_dim_track_artists.sql;
dbt run-operation generate_base_model --args '{"source_name": "spotify", "table_name": "fact_artist_popularity"}' > stg_fact_artist_popularity.sql;
dbt run-operation generate_base_model --args '{"source_name": "spotify", "table_name": "fact_playlist_popularity_and_size"}' > stg_fact_playlist_popularity_and_size.sql;
dbt run-operation generate_base_model --args '{"source_name": "spotify", "table_name": "fact_track_popularity"}' > stg_fact_track_popularity.sql;