with genres_aggregated as (
    select
        playlist_id,
        playlist_name,
        track_genre,
        genre_category,
        count(*) as genre_count,
        sum(count(*)) over(partition by genre_category) as genre_category_count,
        sum(count(*)) over() as total_genres_count
    from 
        {{ ref('int_track_genres__categorized') }}
    group by
        playlist_id,
        playlist_name,
        track_genre,
        genre_category
)

select
    {{ dbt_utils.generate_surrogate_key(['playlist_id', 'track_genre']) }} as table_id,
    playlist_name,
    track_genre,
    genre_category,
    genre_count,
    genre_category_count,
    total_genres_count,
    current_timestamp as table_created_at
from
    genres_aggregated