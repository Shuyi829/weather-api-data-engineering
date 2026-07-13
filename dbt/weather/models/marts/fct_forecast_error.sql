{#
    Forecast-vs-actual error at the hourly grain.

    Only hours present in BOTH series (inner join) can be scored, so this is the
    overlap window between the archive actuals and the forecast's recent-past.
    This is the analytical payoff of the pipeline: how good was the forecast?
#}

with joined as (

    select
        f.location_id,
        f.location_name,
        f.country,
        f.timezone,
        f.observed_hour_utc,
        f.observed_hour_local,
        f.temperature_c        as forecast_temperature_c,
        a.temperature_c        as actual_temperature_c,
        f.wind_speed_kmh       as forecast_wind_speed_kmh,
        a.wind_speed_kmh       as actual_wind_speed_kmh,
        f.shortwave_radiation_wm2 as forecast_shortwave_radiation_wm2,
        a.shortwave_radiation_wm2 as actual_shortwave_radiation_wm2
    from {{ ref('int_weather__forecast_local') }} f
    inner join {{ ref('int_weather__actuals_local') }} a
        on f.location_id = a.location_id
       and f.observed_hour_utc = a.observed_hour_utc

)

select
    {{ surrogate_key(['location_id', 'observed_hour_utc']) }} as forecast_error_key,
    location_id,
    location_name,
    country,
    timezone,
    observed_hour_utc,
    observed_hour_local,
    forecast_temperature_c,
    actual_temperature_c,
    forecast_temperature_c - actual_temperature_c        as temperature_error_c,
    abs(forecast_temperature_c - actual_temperature_c)   as temperature_abs_error_c,
    forecast_wind_speed_kmh,
    actual_wind_speed_kmh,
    forecast_wind_speed_kmh - actual_wind_speed_kmh      as wind_speed_error_kmh,
    forecast_shortwave_radiation_wm2,
    actual_shortwave_radiation_wm2,
    forecast_shortwave_radiation_wm2 - actual_shortwave_radiation_wm2      as shortwave_radiation_error_wm2,
    abs(forecast_shortwave_radiation_wm2 - actual_shortwave_radiation_wm2) as shortwave_radiation_abs_error_wm2
from joined
