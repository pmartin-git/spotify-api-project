with source as (

    select * from {{ source('spotify', 'dim_artist_genres') }}

),

renamed as (

    select
        artist_id,
        artist_name,
        artist_genre,
        artist_genre_order,
        last_updated_at

    from source

)

select 
    {{ dbt_utils.generate_surrogate_key(['artist_id', 'artist_genre']) }} as table_id,
    * 
from renamed

