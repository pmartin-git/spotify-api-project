with source as (

    select * from {{ source('spotify', 'fact_track_popularity') }}

),

renamed as (

    select
        track_id,
        date,
        track_popularity,
        last_updated_at

    from source

)

select * from renamed

