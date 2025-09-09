with source as (

    select * from {{ source('spotify', 'dim_playlists') }}

),

renamed as (

    select
        playlist_id,
        playlist_name,
        playlist_description,
        playlist_owner_id,
        playlist_total_tracks,
        playlist_total_followers,
        playlist_external_url,
        last_updated_at

    from source

)

select * from renamed

