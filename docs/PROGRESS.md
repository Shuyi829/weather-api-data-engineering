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

- **Step 3: dbt on Snowflake.** `snowflake` target in `profiles.yml` (all settings from env vars;
  key-pair or SSO via `SNOWFLAKE_AUTHENTICATOR=externalbrowser`), `to_local_time` macro dispatched per
  adapter. DuckDB 42/42 and Snowflake trial 38/38 pass (`--exclude source:raw`).
  Loader and dbt raw/dbt schema names are configurable (`SNOWFLAKE_RAW_SCHEMA`, `SNOWFLAKE_DBT_SCHEMA`).

## Next: move to a shared learning database

The trial expires mid-November. Target: a learning database in another account, where only one
pre-provisioned role and warehouse exist (no own roles/users/resource monitor, so `00_setup.sql`
does not apply). Point `.env` at it (own user + SSO, one role for loading and dbt, schemas
`WEATHER_RAW` / `WEATHER_*`), run the loader to reload all landing runs, then `dbt build`.

Running dbt with `.env`: dbt does not read `.env`, so load it into the shell first
(PowerShell: `Get-Content ..\..\.env | Where-Object { $_ -match '^[^#].*=' } | ForEach-Object { $k, $v = $_ -split '=', 2; Set-Item "env:$($k.Trim())" $v.Trim() }`).

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
