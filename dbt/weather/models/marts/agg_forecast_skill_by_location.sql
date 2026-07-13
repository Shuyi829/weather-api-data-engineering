{#
    Forecast skill summary per location: standard error metrics over the scored
    overlap window (MAE, bias, RMSE for temperature). This is the headline table
    you'd point a dashboard at.
#}

with errors as (

    select * from {{ ref('fct_forecast_error') }}

)

select
    location_id,
    location_name,
    country,
    count(*)                                              as scored_hours,
    min(observed_hour_utc)                                as first_scored_hour_utc,
    max(observed_hour_utc)                                as last_scored_hour_utc,

    -- temperature skill
    round(avg(temperature_abs_error_c), 3)               as temperature_mae_c,
    round(avg(temperature_error_c), 3)                   as temperature_bias_c,
    round(sqrt(avg(pow(temperature_error_c, 2))), 3)     as temperature_rmse_c,
    round(max(temperature_abs_error_c), 3)               as temperature_max_abs_error_c,

    -- wind skill
    round(avg(abs(wind_speed_error_kmh)), 3)             as wind_speed_mae_kmh,

    -- shortwave radiation skill
    round(avg(shortwave_radiation_abs_error_wm2), 3)     as shortwave_radiation_mae_wm2,
    round(avg(shortwave_radiation_error_wm2), 3)         as shortwave_radiation_bias_wm2,
    round(sqrt(avg(pow(shortwave_radiation_error_wm2, 2))), 3) as shortwave_radiation_rmse_wm2
from errors
group by location_id, location_name, country
