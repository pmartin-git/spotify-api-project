with source as (

    select * from {{ source('spotify', 'dim_track_artists') }}

),

renamed as (

    select
        track_id,
        track_name,
        track_artist_id,
        track_artist_name,
        track_artist_order,
        last_updated_at

    from source

)

select * from renamed

