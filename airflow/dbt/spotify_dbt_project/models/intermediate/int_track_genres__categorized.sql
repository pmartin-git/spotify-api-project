select
    track_genres.table_id,
    track_genres.playlist_id,
    track_genres.playlist_name,
    track_genres.track_id,
    track_genres.track_artist_id,
    track_genres.track_genre,
    coalesce(genre_categories.genre_category, '(uncategorized)') as genre_category,
    current_timestamp as table_created_at
from 
    {{ ref('int_track_genres_from_artists__joined') }} as track_genres
    left join {{ ref('seed__genre_categories__2025_08_20') }} as genre_categories
        on track_genres.track_genre = genre_categories.track_genre
        and track_genres.playlist_id = genre_categories.playlist_id