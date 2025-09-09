with genres_joined as (
    select
        tracks.playlist_id,
        tracks.playlist_name,
        tracks.track_id,
        track_artists.track_artist_id,
        coalesce(artist_genres.artist_genre, '(blank)') as track_genre,
        current_timestamp as table_created_at
    from
        {{ ref('stg_dim_tracks') }} as tracks
        left join {{ ref('stg_dim_track_artists') }} as track_artists
            on tracks.track_id = track_artists.track_id
        left join {{ ref('stg_dim_artist_genres') }} as artist_genres
            on track_artists.track_artist_id = artist_genres.artist_id
)

select
    {{ dbt_utils.generate_surrogate_key(['playlist_id', 'track_id', 'track_artist_id', 'track_genre']) }} as table_id,
    playlist_id,
    playlist_name,
    track_id,
    track_genre,
    track_artist_id,
    table_created_at
from
    genres_joined