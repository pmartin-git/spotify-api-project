select
    track_genre,
    count(*) as genre_count,
    sum(count(*)) over() as total_genres_counted,
    current_timestamp as table_created_at
from 
    {{ ref('int_track_genres_from_artists__joined') }}
group by 
    track_genre