"""Weather data extractor (Extract + Load-to-raw stage).

Mirrors a common production pattern for vendor weather scrapers:

    Vendor API  ->  write files to a landing zone  ->  write a _SUCCESS marker
                ->  load the landed files into the warehouse (raw schema)

Source: Open-Meteo (https://open-meteo.com) - free, no API key required.

For every location in the dbt seed (seeds/locations.csv) we pull two series in
UTC (transformation/timezone handling is deliberately left to dbt):

  * forecast : model forecast incl. the recent past  (api.open-meteo.com)
  * actuals  : historical reanalysis "observed" data  (archive-api.open-meteo.com)

Each batch is tagged with a run_id + ingested_at_utc so downstream dbt staging
models can deduplicate to the latest load (slowly-changing / re-runnable ELT).
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import sys
import time
import uuid
from pathlib import Path

import duckdb
import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LOCATIONS = PROJECT_ROOT / "dbt" / "weather" / "seeds" / "locations.csv"
DEFAULT_DB = PROJECT_ROOT / "data" / "weather.duckdb"
DEFAULT_LANDING = PROJECT_ROOT / "data" / "landing"

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

# Weather variables requested from both endpoints (identical so the two series
# are directly comparable in the forecast-error mart).
HOURLY_VARS = [
    "temperature_2m",
    "relative_humidity_2m",
    "cloud_cover",
    "wind_speed_10m",
    "wind_direction_10m",
    "precipitation",
]
# Additional variables I want to fetch if available.
# NOTE: Open-Meteo's global horizontal radiation field is "shortwave_radiation"
# (there is no "radiation_global" - that name returns HTTP 400).
ADDITIONAL_VARS = [
    "shortwave_radiation",
]

# Every hourly variable we request from the API (core + additional). Both
# endpoints request the same set so the two series stay directly comparable.
ALL_VARS = [*HOURLY_VARS, *ADDITIONAL_VARS]

REQUEST_TIMEOUT = 60
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5


def log(message: str) -> None:
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"{stamp} | {message}", flush=True)


def read_locations(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    if not rows:
        raise ValueError(f"No locations found in {path}")
    return rows


def _get_with_retries(url: str, params: dict) -> dict:
    last_error: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:  # noqa: PERF203
            last_error = exc
            wait = RETRY_BACKOFF_SECONDS * attempt
            log(f"  request failed (attempt {attempt}/{MAX_RETRIES}): {exc} - retrying in {wait}s")
            time.sleep(wait)
    raise RuntimeError(f"Request to {url} failed after {MAX_RETRIES} attempts") from last_error


def _hourly_to_frame(payload: dict, location_id: str, source: str) -> pd.DataFrame:
    hourly = payload.get("hourly")
    if not hourly or not hourly.get("time"):
        log(f"  WARNING: no hourly data returned for {location_id} ({source})")
        return pd.DataFrame()
    frame = pd.DataFrame(hourly)
    frame = frame.rename(columns={"time": "timestamp_utc"})
    frame.insert(0, "location_id", location_id)
    frame["source"] = source
    # Guarantee every expected column exists even if the API omits an all-null one.
    for var in ALL_VARS:
        if var not in frame.columns:
            frame[var] = pd.NA
    ordered = ["location_id", "timestamp_utc", *ALL_VARS, "source"]
    return frame[ordered]


def fetch_forecast(loc: dict, past_days: int, forecast_days: int) -> pd.DataFrame:
    params = {
        "latitude": loc["latitude"],
        "longitude": loc["longitude"],
        "hourly": ",".join(ALL_VARS),
        "past_days": past_days,
        "forecast_days": forecast_days,
        "timezone": "UTC",
    }
    payload = _get_with_retries(FORECAST_URL, params)
    return _hourly_to_frame(payload, loc["location_id"], "forecast")


def fetch_actuals(loc: dict, history_days: int) -> pd.DataFrame:
    # Archive reanalysis lags "now" by a few days; end a couple of days back so
    # we request a window that is actually populated.
    end = dt.date.today() - dt.timedelta(days=2)
    start = end - dt.timedelta(days=history_days)
    params = {
        "latitude": loc["latitude"],
        "longitude": loc["longitude"],
        "hourly": ",".join(ALL_VARS),
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "timezone": "UTC",
    }
    payload = _get_with_retries(ARCHIVE_URL, params)
    return _hourly_to_frame(payload, loc["location_id"], "actuals")


def write_landing(frame: pd.DataFrame, landing_dir: Path, source: str, location_id: str) -> None:
    if frame.empty:
        return
    target = landing_dir / source
    target.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(target / f"{location_id}.parquet", index=False)


def load_to_duckdb(db_path: Path, frame: pd.DataFrame, table: str) -> int:
    if frame.empty:
        return 0
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS raw;")
        con.register("incoming", frame)
        con.execute(f"CREATE TABLE IF NOT EXISTS raw.{table} AS SELECT * FROM incoming WHERE 1=0;")
        con.execute(f"INSERT INTO raw.{table} SELECT * FROM incoming;")
        count = con.execute(f"SELECT count(*) FROM raw.{table};").fetchone()[0]
    finally:
        con.close()
    return count


def run(args: argparse.Namespace) -> int:
    run_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]
    ingested_at = dt.datetime.now(dt.timezone.utc)
    landing_dir = Path(args.landing) / run_id

    log(f"Starting extract run_id={run_id}")
    locations = read_locations(Path(args.locations))
    log(f"Loaded {len(locations)} locations from {args.locations}")

    forecast_frames: list[pd.DataFrame] = []
    actuals_frames: list[pd.DataFrame] = []

    for loc in locations:
        loc_id = loc["location_id"]
        log(f"Fetching {loc_id} ({loc['location_name']})")
        try:
            if args.mode in ("all", "forecast"):
                fc = fetch_forecast(loc, args.past_days, args.forecast_days)
                write_landing(fc, landing_dir, "forecast", loc_id)
                forecast_frames.append(fc)
            if args.mode in ("all", "actuals"):
                ac = fetch_actuals(loc, args.history_days)
                write_landing(ac, landing_dir, "actuals", loc_id)
                actuals_frames.append(ac)
        except Exception as exc:  # keep going; one bad location shouldn't kill the batch
            log(f"  ERROR fetching {loc_id}: {exc}")

    def _finalize(frames: list[pd.DataFrame]) -> pd.DataFrame:
        frames = [f for f in frames if not f.empty]
        if not frames:
            return pd.DataFrame()
        out = pd.concat(frames, ignore_index=True)
        out["run_id"] = run_id
        out["ingested_at_utc"] = ingested_at
        return out

    forecast_all = _finalize(forecast_frames)
    actuals_all = _finalize(actuals_frames)

    total_fc = load_to_duckdb(Path(args.db), forecast_all, "forecast")
    total_ac = load_to_duckdb(Path(args.db), actuals_all, "actuals")

    # Sentinel / _SUCCESS marker - the "data is ready" signal for the load step.
    landing_dir.mkdir(parents=True, exist_ok=True)
    marker = landing_dir / "_SUCCESS"
    marker.write_text(
        f"run_id={run_id}\n"
        f"completed_at_utc={dt.datetime.now(dt.timezone.utc).isoformat()}\n"
        f"forecast_rows_this_batch={0 if forecast_all.empty else len(forecast_all)}\n"
        f"actuals_rows_this_batch={0 if actuals_all.empty else len(actuals_all)}\n",
        encoding="utf-8",
    )

    log(f"Landed files under: {landing_dir}")
    log(f"raw.forecast total rows: {total_fc}")
    log(f"raw.actuals total rows:  {total_ac}")
    log(f"Wrote success marker: {marker}")
    log("Extract complete.")
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Extract Open-Meteo weather data into the DuckDB raw schema.")
    p.add_argument("--mode", choices=["all", "forecast", "actuals"], default="all",
                   help="Which series to pull (default: all).")
    p.add_argument("--locations", default=str(DEFAULT_LOCATIONS), help="Path to locations CSV.")
    p.add_argument("--db", default=str(DEFAULT_DB), help="Path to the DuckDB database file.")
    p.add_argument("--landing", default=str(DEFAULT_LANDING), help="Landing-zone root folder.")
    p.add_argument("--past-days", type=int, default=7, help="Forecast endpoint: days of recent past to include.")
    p.add_argument("--forecast-days", type=int, default=16, help="Forecast endpoint: days ahead to fetch.")
    p.add_argument("--history-days", type=int, default=30, help="Archive endpoint: days of history to fetch.")
    return p.parse_args(argv)


if __name__ == "__main__":
    sys.exit(run(parse_args()))
