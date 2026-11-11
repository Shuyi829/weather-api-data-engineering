# Progress: DuckDB → Snowflake port

Working notes for continuing the Snowflake migration on another machine.

## Done

- **Step 0: CI fixed.** `dbt CI` passes again (data dir created, radiation column added to the empty raw tables).
- **Step 1: Snowflake account setup.** `snowflake/00_setup.sql` has been run: resource monitor,
  `WEATHER_WH` (XS, auto-suspend 60 s), `WEATHER.RAW`, roles `WEATHER_LOADER` / `WEATHER_TRANSFORMER`,
  service user `WEATHER_SVC` with key-pair auth.
- **Step 2: Loader.** `extract/load_snowflake.py` PUTs each completed landing run (gated on `_SUCCESS`)
  to `@raw.landing` and runs `COPY INTO`. All 37 historical runs are loaded
  (`RAW.FORECAST` 551,448 rows, `RAW.ACTUALS` 743,256 rows). A re-run loads 0 rows (idempotent).

## Next: Step 3, dbt on Snowflake

1. `pip install dbt-snowflake==1.9.4` (matches dbt-core 1.9.1). This downgrades
   `snowflake-connector-python` to 3.18.1; update `requirements.txt` to pin that version.
2. Add a `snowflake` output to `dbt/weather/profiles.yml` (type snowflake, key-pair via
   `private_key_path`, role `WEATHER_TRANSFORMER`, database `WEATHER`, schema `dbt`), reading
   settings from env vars. Keep `target: dev` (DuckDB) as the default.
3. Add `macros/to_local_time.sql` using `adapter.dispatch`:
   `default__` = `(ts at time zone 'UTC') at time zone tz` (DuckDB),
   `snowflake__` = `convert_timezone('UTC', tz, ts)`.
   Use it in both `int_weather__*_local.sql` models. This is the only DuckDB-specific SQL.
   (Checked: Snowflake parses `timestamp_utc` and `convert_timezone` gives the correct DST offsets.)
4. `dbt build --profiles-dir .` on DuckDB must still pass.
5. dbt doesn't read `.env`: load it into the shell, then
   `dbt debug --target snowflake` and `dbt build --target snowflake --exclude source:raw`.
6. Compare `DBT_MARTS.AGG_FORECAST_SKILL_BY_LOCATION` with DuckDB. Small differences are expected:
   Snowflake has all 37 runs and DuckDB only the 20 since the radiation reset.

## Later

- **Step 4:** incremental models (`merge`), zero-copy clone for CI, Time Travel, cost via `QUERY_HISTORY`.
- **Step 5:** Dagster target switch (`DBT_TARGET`), README "DuckDB → Snowflake port" section.

## Setting up a new machine

Not in git (by design): `.env`, the private key, `data/` (landing zone + DuckDB file).

- Clone, create `.venv`, `pip install -r requirements.txt`.
- Key: prefer a **new key pair per machine**. Generate it as in `snowflake/README.md`, then as
  ACCOUNTADMIN run `alter user weather_svc set rsa_public_key_2 = '<new public key>';`
  (Snowflake allows two active keys, so both machines work).
- Copy `.env.example` to `.env`, set `SNOWFLAKE_ACCOUNT` and the key path.
- `data/` is optional for Step 3: the raw data is already in Snowflake. For local DuckDB, either
  copy `data/` across or run `python extract/extract_weather.py --mode all` for fresh data.
