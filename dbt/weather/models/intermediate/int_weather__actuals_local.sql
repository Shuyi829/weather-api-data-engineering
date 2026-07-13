{#
    Convert UTC observation hours to DST-aware local wall-clock time.

    The naive UTC timestamp is first stamped as UTC, then converted to each
    location's IANA timezone. DuckDB's ICU extension handles DST transitions,
    so e.g. Europe/London hours shift by +0/+1 across BST automatically and
    Asia/Kolkata keeps its +05:30 half-hour offset.

    This mirrors the timezone handling in the production weather scrapers,
    but expressed declaratively in SQL instead of per-script Python.
#}

with actuals as (

    select * from {{ ref('stg_weather__actuals') }}

),

locations as (

    select location_id, location_name, country, timezone
    from {{ ref('stg_weather__locations') }}

)

select
    a.actual_key,
    a.location_id,
    l.location_name,
    l.country,
    l.timezone,
    a.observed_hour_utc,
    (a.observed_hour_utc at time zone 'UTC') at time zone l.timezone as observed_hour_local,
    a.temperature_c,
    a.relative_humidity_pct,
    a.cloud_cover_pct,
    a.wind_speed_kmh,
    a.wind_direction_deg,
    a.precipitation_mm,
    a.shortwave_radiation_wm2
from actuals a
inner join locations l on a.location_id = l.location_id
