with source as (

    select * from {{ source('spotify', 'dim_artists') }}

),

renamed as (

    select
        artist_id,
        artist_name,
        artist_genres,
        artist_popularity,
        artist_total_followers,
        artist_external_url,
        last_updated_at

    from source

)

select * from renamed

