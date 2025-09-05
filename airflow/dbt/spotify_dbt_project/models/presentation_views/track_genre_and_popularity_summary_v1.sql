select
    playlist_name,
    track_name,
    track_artist_name,
    track_album_name,
    genre_categories,
    genre_category_sub_genres,
    track_popularity
from
    {{ ref('track_genre_and_popularity_summary_t1') }}
order by
    track_popularity desc