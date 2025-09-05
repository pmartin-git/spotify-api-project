select
    playlist_name,
    genre_category,
    genre_category_count,
    genre_category_pct,
    genre_category_sub_genres
from
    {{ ref('playlist_genre_summary_t1') }}