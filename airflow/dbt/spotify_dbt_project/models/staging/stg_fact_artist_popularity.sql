with source as (

    select * from {{ source('spotify', 'fact_artist_popularity') }}

),

renamed as (

    select
        artist_id,
        date,
        artist_popularity,
        artist_total_followers,
        last_updated_at

    from source

)

select 
    {{ dbt_utils.generate_surrogate_key(['artist_id', 'date']) }} as table_id,
    * 
from renamed

