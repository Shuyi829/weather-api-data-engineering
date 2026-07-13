{#
    Consumption-ready hourly weather fact.

    One row per location per hour, with actual and forecast measures side by side
    (full outer join so hours that exist in only one series are still present).
    Local, DST-aware timestamps are carried through for BI / reporting.
#}

with actuals as (

    select * from {{ ref('int_weather__actuals_local') }}

),

forecast as (

    select * from {{ ref('int_weather__forecast_local') }}

)

select
    {{ surrogate_key(['coalesce(a.location_id, f.location_id)', 'coalesce(a.observed_hour_utc, f.observed_hour_utc)']) }} as hourly_weather_key,
    coalesce(a.location_id, f.location_id)               as location_id,
    coalesce(a.location_name, f.location_name)           as location_name,
    coalesce(a.country, f.country)                        as country,
    coalesce(a.timezone, f.timezone)                     as timezone,
    coalesce(a.observed_hour_utc, f.observed_hour_utc)   as observed_hour_utc,
    coalesce(a.observed_hour_local, f.observed_hour_local) as observed_hour_local,

    -- actual measures
    a.temperature_c        as actual_temperature_c,
    a.relative_humidity_pct as actual_relative_humidity_pct,
    a.cloud_cover_pct      as actual_cloud_cover_pct,
    a.wind_speed_kmh       as actual_wind_speed_kmh,
    a.precipitation_mm     as actual_precipitation_mm,
    a.shortwave_radiation_wm2 as actual_shortwave_radiation_wm2,

    -- forecast measures
    f.temperature_c        as forecast_temperature_c,
    f.relative_humidity_pct as forecast_relative_humidity_pct,
    f.cloud_cover_pct      as forecast_cloud_cover_pct,
    f.wind_speed_kmh       as forecast_wind_speed_kmh,
    f.precipitation_mm     as forecast_precipitation_mm,
    f.shortwave_radiation_wm2 as forecast_shortwave_radiation_wm2,

    (a.actual_key is not null)   as has_actual,
    (f.forecast_key is not null) as has_forecast
from actuals a
full outer join forecast f
    on a.location_id = f.location_id
   and a.observed_hour_utc = f.observed_hour_utc
