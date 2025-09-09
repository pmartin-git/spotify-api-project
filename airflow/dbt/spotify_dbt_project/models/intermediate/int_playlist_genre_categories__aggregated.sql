with genre_categories_aggregated as (
    select
        playlist_name,
        genre_category,
        genre_category_count,
        total_genres_count,
        array_agg(distinct track_genre order by track_genre) as genre_category_sub_genres
    from
        {{ ref('int_playlist_genres__aggregated') }}
    group by
        playlist_name,
        genre_category,
        genre_category_count,
        total_genres_count
)

select
    {{ dbt_utils.generate_surrogate_key(['playlist_name', 'genre_category']) }} as table_id,
    playlist_name,
    genre_category,
    genre_category_count,
    total_genres_count,
    genre_category_sub_genres,
    current_timestamp as table_created_at
from
    genre_categories_aggregated