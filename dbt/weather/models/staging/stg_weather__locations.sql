with source as (

    select * from {{ ref('locations') }}

)

select
    location_id,
    location_name,
    country,
    cast(latitude as double)  as latitude,
    cast(longitude as double) as longitude,
    timezone
from source
