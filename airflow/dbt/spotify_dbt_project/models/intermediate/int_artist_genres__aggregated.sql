select
    playlist_id,
    playlist_name,
    track_artist_id as artist_id,
    array_agg(distinct genre_category order by genre_category) as genre_categories,
    array_agg(distinct track_genre order by track_genre) as genre_category_sub_genres,
    current_timestamp as table_created_at
from
    {{ ref('int_track_genres__categorized') }}
group by
    playlist_id,
    playlist_name,
    artist_id