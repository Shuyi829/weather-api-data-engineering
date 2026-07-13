{#
    Local-time forecast, DST-aware (see int_weather__actuals_local for details).
#}

with forecast as (

    select * from {{ ref('stg_weather__forecast') }}

),

locations as (

    select location_id, location_name, country, timezone
    from {{ ref('stg_weather__locations') }}

)

select
    f.forecast_key,
    f.location_id,
    l.location_name,
    l.country,
    l.timezone,
    f.observed_hour_utc,
    (f.observed_hour_utc at time zone 'UTC') at time zone l.timezone as observed_hour_local,
    f.temperature_c,
    f.relative_humidity_pct,
    f.cloud_cover_pct,
    f.wind_speed_kmh,
    f.wind_direction_deg,
    f.precipitation_mm,
    f.shortwave_radiation_wm2
from forecast f
inner join locations l on f.location_id = l.location_id
