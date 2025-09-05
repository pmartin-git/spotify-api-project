select
    artist_genres.playlist_id,
    artist_genres.playlist_name,
    artist_genres.artist_id,
    artists.artist_name,
    artist_genres.genre_categories,
    artist_genres.genre_category_sub_genres,
    artists.artist_popularity,
    current_timestamp as table_created_at
from
    {{ ref('int_artist_genres__aggregated') }} as artist_genres
    left join {{ ref('stg_dim_artists') }} as artists
        on artist_genres.artist_id = artists.artist_id