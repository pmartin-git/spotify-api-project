select
    table_id,
    playlist_name,
    genre_category,
    genre_category_count,
    round(genre_category_count / total_genres_count, 3)  as genre_category_pct,
    genre_category_sub_genres,
    current_timestamp as table_created_at
from
    {{ ref('int_track_genre_categories__aggregated') }}