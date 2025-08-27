This Airflow project populates tables in an Amazon RDS Postgres database with data pulled from the Spotify Web API. The relevant table DDL is below.

**Postgres table creation DDL**
CREATE TABLE spotify_sources.dim_artists (
	artist_id varchar NULL,
	artist_name varchar NULL,
	artist_genres json NULL,
	artist_popularity numeric NULL,
	artist_total_followers int4 NULL,
	artist_external_url varchar NULL,
	last_updated_at timestamp NULL,
	CONSTRAINT dim_artists_unique UNIQUE (artist_id)
);

CREATE TABLE spotify_sources.dim_artist_genres (
	artist_id varchar NULL,
	artist_name varchar NULL,
	artist_genre varchar NULL,
	artist_genre_order int4 NULL,
	last_updated_at timestamp NULL,
	CONSTRAINT dim_artist_genres_unique UNIQUE (artist_id, artist_genre)
);

CREATE TABLE spotify_sources.dim_playlists (
	playlist_id varchar NULL,
	playlist_name varchar NULL,
	playlist_description varchar NULL,
	playlist_owner_id varchar NULL,
	playlist_total_tracks int4 NULL,
	playlist_total_followers int4 NULL,
	playlist_external_url varchar NULL,
	last_updated_at timestamp NULL,
	CONSTRAINT dim_playlists_unique UNIQUE (playlist_id)
);

CREATE TABLE spotify_sources.dim_tracks (
	track_id varchar NULL,
	track_name varchar NULL,
	track_album_id varchar NULL,
	track_album_name varchar NULL,
	track_artist_id varchar NULL,
	track_artist_name varchar NULL,
	track_additional_artists json NULL,
	playlist_id varchar NULL,
	playlist_name varchar NULL,
	track_added_to_playlist_at timestamptz NULL,
	track_popularity numeric NULL,
	track_external_url varchar NULL,
	last_updated_at timestamp NULL,
	CONSTRAINT dim_tracks_unique UNIQUE (track_id)
);

CREATE TABLE spotify_sources.fact_artist_popularity (
	artist_id varchar NULL,
	"date" date NULL,
	artist_popularity numeric NULL,
	artist_total_followers int4 NULL,
	last_updated_at timestamp NULL,
	CONSTRAINT fact_artist_popularity_unique UNIQUE (artist_id, date)
);

CREATE TABLE spotify_sources.fact_track_popularity (
	track_id varchar NULL,
	"date" date NULL,
	track_popularity numeric NULL,
	last_updated_at timestamp NULL,
	CONSTRAINT fact_track_popularity_unique UNIQUE (track_id, date)
);

CREATE TABLE spotify_sources.fact_playlist_popularity_and_size (
	playlist_id varchar NULL,
	"date" date NULL,
	playlist_total_tracks int4 NULL,
	playlist_total_followers int4 NULL,
	last_updated_at timestamp NULL,
	CONSTRAINT fact_playlist_popularity_and_size_unique UNIQUE (playlist_id, date)
);