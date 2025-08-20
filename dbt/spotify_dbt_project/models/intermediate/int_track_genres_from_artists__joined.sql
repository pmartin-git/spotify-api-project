select
    track_artists.track_id,
    coalesce(artist_genres.artist_genre, '(blank)') as track_genre,
    current_timestamp as table_created_at
from
    {{ ref('stg_dim_track_artists') }} as track_artists
    left join {{ ref('stg_dim_artist_genres') }} as artist_genres
        on track_artists.track_artist_id = artist_genres.artist_id