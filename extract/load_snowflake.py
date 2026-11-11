"""Load landed weather batches into Snowflake (the Load step for the Snowflake target).

Each extractor run leaves a folder in the landing zone:

    data/landing/<run_id>/forecast/<location_id>.parquet
    data/landing/<run_id>/actuals/<location_id>.parquet
    data/landing/<run_id>/_SUCCESS

For every run folder that has a _SUCCESS marker, this script:

  1. PUTs the Parquet files into an internal stage at @landing/<run_id>/<source>/
  2. runs COPY INTO <source>, deriving run_id and ingested_at_utc from the
     staged file path (the run_id starts with the UTC time the run began)

COPY INTO keeps per-table load metadata and skips files it has already loaded,
so re-running over the whole landing zone is safe (idempotent) and only new
runs are actually loaded.

Usage:

    python extract/load_snowflake.py --check                 # connection test only
    python extract/load_snowflake.py                         # load every completed run
    python extract/load_snowflake.py --run-id 20260714T160015Z_bbeaaafe

Connection settings come from .env (see .env.example).
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LANDING = PROJECT_ROOT / "data" / "landing"
SOURCES = ("forecast", "actuals")

# Raw columns as delivered by the API, in landing-file order.
VALUE_COLUMNS = [
    "temperature_2m",
    "relative_humidity_2m",
    "cloud_cover",
    "wind_speed_10m",
    "wind_direction_10m",
    "precipitation",
    "shortwave_radiation",
]

RAW_TABLE_DDL = """
create table if not exists {table} (
    location_id          varchar,
    timestamp_utc        varchar,
    {value_columns},
    source               varchar,
    run_id               varchar,
    ingested_at_utc      timestamp_ntz,
    _source_file         varchar,
    _loaded_at           timestamp_ltz default current_timestamp()
)
"""

# Parquet columns are read from the $1 variant; run_id is the first folder of
# the staged path and its first 16 chars are the run's UTC start time.
COPY_SQL = """
copy into {table} (
    location_id, timestamp_utc, {value_columns}, source,
    run_id, ingested_at_utc, _source_file
)
from (
    select
        $1:location_id::varchar,
        $1:timestamp_utc::varchar,
        {value_selects},
        $1:source::varchar,
        split_part(metadata$filename, '/', 1),
        to_timestamp_ntz(left(split_part(metadata$filename, '/', 1), 16), 'YYYYMMDD"T"HH24MISS"Z"'),
        metadata$filename
    from @landing/{run_id}/{table}/
)
file_format = (format_name = 'parquet_format')
"""


def log(message: str) -> None:
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"{stamp} | {message}", flush=True)


def connect() -> snowflake.connector.SnowflakeConnection:
    load_dotenv(PROJECT_ROOT / ".env")
    missing = [k for k in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER") if not os.getenv(k)]
    key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
    authenticator = os.getenv("SNOWFLAKE_AUTHENTICATOR")
    if not key_path and not authenticator:
        missing.append("SNOWFLAKE_PRIVATE_KEY_PATH or SNOWFLAKE_AUTHENTICATOR")
    if missing:
        raise SystemExit(f"Missing settings in .env: {', '.join(missing)} (see .env.example)")
    # Key-pair auth (service user) if a key is configured, otherwise e.g. SSO
    # via SNOWFLAKE_AUTHENTICATOR=externalbrowser (opens a browser to log in).
    auth = {"private_key_file": key_path} if key_path else {
        "authenticator": authenticator,
        "client_store_temporary_credential": True,  # cache the SSO token between runs
    }
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        role=os.getenv("SNOWFLAKE_LOADER_ROLE", "WEATHER_LOADER"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "WEATHER_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "WEATHER"),
        **auth,
    )


def raw_schema() -> str:
    return os.getenv("SNOWFLAKE_RAW_SCHEMA", "RAW")


def ensure_objects(cur) -> None:
    """Create the raw schema, file format, stage and raw tables if they don't exist yet.

    Everything below is unqualified, so it lands in the raw schema made current here.
    """
    # Snowflake checks CREATE SCHEMA even with IF NOT EXISTS, and a least-privilege
    # loader role may not have it, so only create the schema when it's missing.
    try:
        cur.execute(f"use schema {raw_schema()}")
    except snowflake.connector.errors.ProgrammingError:
        cur.execute(f"create schema {raw_schema()}")
        cur.execute(f"use schema {raw_schema()}")
    cur.execute("create file format if not exists parquet_format type = parquet")
    cur.execute(
        "create stage if not exists landing "
        "file_format = (format_name = 'parquet_format') "
        "comment = 'Landing-zone Parquet files, one folder per extractor run'"
    )
    value_ddl = ",\n    ".join(f"{c:<20} double" for c in VALUE_COLUMNS)
    for table in SOURCES:
        cur.execute(RAW_TABLE_DDL.format(table=table, value_columns=value_ddl))


def completed_runs(landing: Path, run_id: str | None) -> list[Path]:
    if run_id:
        runs = [landing / run_id]
    else:
        runs = sorted(p for p in landing.iterdir() if p.is_dir())
    ready = [r for r in runs if (r / "_SUCCESS").exists()]
    for r in sorted(set(runs) - set(ready)):
        log(f"Skipping {r.name}: no _SUCCESS marker (run incomplete or still writing)")
    return ready


def load_run(cur, run_dir: Path) -> dict[str, int]:
    run_id = run_dir.name
    loaded: dict[str, int] = {}
    value_selects = ",\n        ".join(f"$1:{c}::double" for c in VALUE_COLUMNS)
    for table in SOURCES:
        files_dir = run_dir / table
        if not any(files_dir.glob("*.parquet")):
            continue
        # Parquet is already compressed, so skip gzip; files already staged are left alone.
        local = files_dir.as_posix()
        cur.execute(f"put 'file://{local}/*.parquet' @landing/{run_id}/{table}/ auto_compress = false")
        cur.execute(
            COPY_SQL.format(
                table=table,
                run_id=run_id,
                value_columns=", ".join(VALUE_COLUMNS),
                value_selects=value_selects,
            )
        )
        # One result row per file (file, status, rows_parsed, rows_loaded, ...). If every
        # file was already loaded, Snowflake returns a single "0 files processed" row.
        rows = [r for r in cur.fetchall() if len(r) > 3 and r[1] == "LOADED"]
        loaded[table] = sum(r[3] for r in rows)
    return loaded


def run(args: argparse.Namespace) -> int:
    con = connect()
    cur = con.cursor()
    try:
        cur.execute("select current_user(), current_role(), current_warehouse(), current_database()")
        user, role, wh, db = cur.fetchone()
        log(f"Connected as {user} (role={role}, warehouse={wh}, database={db}, raw schema={raw_schema()})")
        if args.check:
            return 0

        ensure_objects(cur)
        runs = completed_runs(Path(args.landing), args.run_id)
        log(f"{len(runs)} completed run(s) to check")
        totals = {t: 0 for t in SOURCES}
        for run_dir in runs:
            loaded = load_run(cur, run_dir)
            for table, n in loaded.items():
                totals[table] += n
            if any(loaded.values()):
                log(f"  {run_dir.name}: " + ", ".join(f"{t}={n}" for t, n in loaded.items()))
        for table in SOURCES:
            cur.execute(f"select count(*) from {table}")
            log(f"{raw_schema()}.{table}: +{totals[table]} rows this run, {cur.fetchone()[0]} total")
    finally:
        cur.close()
        con.close()
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Load landed weather batches into the Snowflake raw schema.")
    p.add_argument("--landing", default=str(DEFAULT_LANDING), help="Landing-zone root folder.")
    p.add_argument("--run-id", help="Load only this run folder (default: every completed run).")
    p.add_argument("--check", action="store_true", help="Only test the connection.")
    return p.parse_args(argv)


if __name__ == "__main__":
    sys.exit(run(parse_args()))
