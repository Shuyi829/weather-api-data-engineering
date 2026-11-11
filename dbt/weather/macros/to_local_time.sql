{#
    Convert a naive UTC timestamp to naive local wall-clock time in an IANA
    timezone (DST-aware). Dispatched per adapter so the same models run on
    DuckDB (ICU extension) and Snowflake.
#}
{% macro to_local_time(utc_ts, tz) %}
    {{ return(adapter.dispatch('to_local_time')(utc_ts, tz)) }}
{% endmacro %}

{% macro default__to_local_time(utc_ts, tz) %}
    ({{ utc_ts }} at time zone 'UTC') at time zone {{ tz }}
{% endmacro %}

{% macro snowflake__to_local_time(utc_ts, tz) %}
    convert_timezone('UTC', {{ tz }}, {{ utc_ts }})
{% endmacro %}
