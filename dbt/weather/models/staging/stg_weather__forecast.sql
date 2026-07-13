with source as (

    select * from {{ source('raw', 'forecast') }}

),

typed as (

    select
        location_id,
        cast(timestamp_utc as timestamp)      as observed_hour_utc,
        cast(temperature_2m as double)        as temperature_c,
        cast(relative_humidity_2m as double)  as relative_humidity_pct,
        cast(cloud_cover as double)           as cloud_cover_pct,
        cast(wind_speed_10m as double)        as wind_speed_kmh,
        cast(wind_direction_10m as double)    as wind_direction_deg,
        cast(precipitation as double)         as precipitation_mm,
        cast(shortwave_radiation as double)   as shortwave_radiation_wm2,
        run_id,
        cast(ingested_at_utc as timestamp)    as ingested_at_utc
    from source

),

-- Keep only the most recent batch for each location/hour so re-runs are idempotent.
deduped as (

    select
        *,
        row_number() over (
            partition by location_id, observed_hour_utc
            order by ingested_at_utc desc
        ) as _row_rank
    from typed

)

select
    {{ surrogate_key(['location_id', 'observed_hour_utc']) }} as forecast_key,
    location_id,
    observed_hour_utc,
    temperature_c,
    relative_humidity_pct,
    cloud_cover_pct,
    wind_speed_kmh,
    wind_direction_deg,
    precipitation_mm,
    shortwave_radiation_wm2,
    run_id,
    ingested_at_utc
from deduped
where _row_rank = 1
