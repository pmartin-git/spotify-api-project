select
    tracks.playlist_id,
    tracks.playlist_name,
    tracks.track_id,
    tracks.track_name,
    tracks.track_artist_name,
    tracks.track_album_name,
    genres.genre_categories,
    genres.genre_category_sub_genres,
    tracks.track_popularity
from
    {{ ref('stg_dim_tracks') }} as tracks
    left join {{ ref('int_track_genres__aggregated') }} as genres
        on tracks.track_id = genres.track_id