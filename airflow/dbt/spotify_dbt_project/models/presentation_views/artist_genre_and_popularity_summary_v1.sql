select
    playlist_name,
    artist_name,
    genre_categories,
    genre_category_sub_genres,
    artist_popularity,
    current_timestamp as table_created_at
from
    {{ ref('artist_genre_and_popularity_summary_t1') }}
order by
    artist_popularity desc