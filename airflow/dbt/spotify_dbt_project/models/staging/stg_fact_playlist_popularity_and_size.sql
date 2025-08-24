with source as (

    select * from {{ source('spotify', 'fact_playlist_popularity_and_size') }}

),

renamed as (

    select
        playlist_id,
        date,
        playlist_total_tracks,
        playlist_total_followers,
        last_updated_at

    from source

)

select 
    {{ dbt_utils.generate_surrogate_key(['playlist_id', 'date']) }} as table_id,
    * 
from renamed

