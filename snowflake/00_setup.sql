-- =============================================================================
-- One-time Snowflake setup for the weather ELT pipeline.
--
-- Run top to bottom in a Snowsight SQL worksheet as ACCOUNTADMIN. Safe to
-- re-run: every statement is CREATE ... IF NOT EXISTS or an idempotent GRANT.
--
-- Object model (least privilege):
--
--   WEATHER_LOADER       writes raw batches into WEATHER.RAW   (Python extractor)
--   WEATHER_TRANSFORMER  reads RAW, builds dbt schemas         (dbt)
--   WEATHER_SVC          service user (key-pair auth) holding both roles
--   WEATHER_WH           X-Small warehouse, suspends after 60s idle
--   WEATHER_MONITOR      resource monitor capping monthly credit spend
--
-- Both roles roll up to SYSADMIN, so admins can see and manage everything the
-- pipeline creates.
-- =============================================================================

use role accountadmin;

-- -----------------------------------------------------------------------------
-- 1. Cost guardrail: cap credits so a runaway query can't drain the trial.
--    1 credit on an XS warehouse = 1 hour of running. 20/month is plenty here.
-- -----------------------------------------------------------------------------
create resource monitor if not exists weather_monitor
    with credit_quota = 20
    frequency = monthly
    start_timestamp = immediately
    triggers
        on 75 percent do notify
        on 100 percent do suspend
        on 110 percent do suspend_immediate;

-- -----------------------------------------------------------------------------
-- 2. Compute
-- -----------------------------------------------------------------------------
use role sysadmin;

create warehouse if not exists weather_wh
    warehouse_size = xsmall
    auto_suspend = 60           -- seconds idle before suspending (billing stops)
    auto_resume = true
    initially_suspended = true
    comment = 'Weather ELT pipeline: loads + dbt';

use role accountadmin;
alter warehouse weather_wh set resource_monitor = weather_monitor;

-- -----------------------------------------------------------------------------
-- 3. Storage
-- -----------------------------------------------------------------------------
use role sysadmin;

create database if not exists weather comment = 'Weather ELT pipeline';
create schema if not exists weather.raw comment = 'Raw Open-Meteo batches, loaded as-is';

-- -----------------------------------------------------------------------------
-- 4. Roles
-- -----------------------------------------------------------------------------
use role securityadmin;

create role if not exists weather_loader      comment = 'Writes raw batches';
create role if not exists weather_transformer comment = 'Runs dbt';

grant role weather_loader      to role sysadmin;
grant role weather_transformer to role sysadmin;

-- Both roles need compute and to see the database.
grant usage on warehouse weather_wh to role weather_loader;
grant usage on warehouse weather_wh to role weather_transformer;
grant usage on database weather     to role weather_loader;
grant usage on database weather     to role weather_transformer;

-- Loader: owns what lands in RAW (tables, stages, file formats).
grant usage on schema weather.raw to role weather_loader;
grant create table, create stage, create file format
    on schema weather.raw to role weather_loader;

-- Transformer: read-only on RAW (including tables created later) ...
grant usage on schema weather.raw to role weather_transformer;
grant select on all tables    in schema weather.raw to role weather_transformer;
grant select on future tables in schema weather.raw to role weather_transformer;
-- ... and creates its own schemas (staging/intermediate/marts) for dbt.
grant create schema on database weather to role weather_transformer;

-- -----------------------------------------------------------------------------
-- 5. Service user (key-pair auth, no password)
--    Generate the key pair first (see snowflake/README.md), then paste the
--    PUBLIC key body (without the BEGIN/END lines or newlines) below.
-- -----------------------------------------------------------------------------
use role useradmin;

create user if not exists weather_svc
    type = service
    default_role = weather_transformer
    default_warehouse = weather_wh
    default_namespace = weather.raw
    comment = 'Weather pipeline: extractor + dbt';

alter user weather_svc set rsa_public_key = '<PASTE_PUBLIC_KEY_HERE>';

use role securityadmin;
grant role weather_loader      to user weather_svc;
grant role weather_transformer to user weather_svc;

-- -----------------------------------------------------------------------------
-- 6. Sanity checks
-- -----------------------------------------------------------------------------
use role accountadmin;
show grants to role weather_loader;
show grants to role weather_transformer;
show grants to user weather_svc;
desc user weather_svc;   -- RSA_PUBLIC_KEY_FP should be populated
