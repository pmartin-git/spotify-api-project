with source as (

    select * from {{ source('spotify', 'dim_tracks') }}

),

renamed as (

    select
        track_id,
        track_name,
        track_album_id,
        track_album_name,
        track_artist_id,
        track_artist_name,
        track_additional_artists,
        playlist_id,
        playlist_name,
        track_added_to_playlist_at,
        track_popularity,
        track_external_url,
        last_updated_at

    from source

)

select * from renamed

