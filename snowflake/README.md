# Snowflake setup

One-time setup to run this pipeline on Snowflake alongside the local DuckDB target.

## 1. Generate a key pair for the service user

The pipeline authenticates as `WEATHER_SVC` with a key pair (no password). Keep the
keys **outside the repo**, e.g. in `~/.snowflake/keys/`. In Git Bash:

```bash
mkdir -p ~/.snowflake/keys && cd ~/.snowflake/keys
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out weather_svc_key.p8 -nocrypt
openssl rsa -in weather_svc_key.p8 -pubout -out weather_svc_key.pub
```

- `weather_svc_key.p8`: **private** key. Never share or commit it.
- `weather_svc_key.pub`: public key. Its body goes into Snowflake.

## 2. Run the setup script

1. Open `00_setup.sql`. Replace `<PASTE_PUBLIC_KEY_HERE>` with the contents of
   `weather_svc_key.pub`, without the `-----BEGIN/END PUBLIC KEY-----` lines,
   joined onto one line.
2. In Snowsight: **Projects → Workspaces → + → SQL File**, paste the script, then
   run all of it (Ctrl+Shift+Enter) as `ACCOUNTADMIN`.
3. Check the last result: `DESC USER WEATHER_SVC` should show a value for `RSA_PUBLIC_KEY_FP`.

## 3. Fill in `.env`

Copy `.env.example` to `.env` in the project root and fill in `SNOWFLAKE_ACCOUNT`.
To find it in Snowsight, click your name (bottom left), then **Account → View account details**,
and copy the **Account identifier**. It looks like `ORGNAME-ACCOUNTNAME`.

## What the script creates

| Object | Purpose |
|---|---|
| `WEATHER_MONITOR` | Resource monitor: notify at 75%, suspend at 100% of 20 credits/month |
| `WEATHER_WH` | X-Small warehouse, auto-suspends after 60 s idle |
| `WEATHER.RAW` | Landing schema for raw Open-Meteo batches |
| `WEATHER_LOADER` | Role that writes to `RAW` (extractor) |
| `WEATHER_TRANSFORMER` | Role that reads `RAW` and builds dbt schemas |
| `WEATHER_SVC` | Service user (key-pair only) that holds both roles |
