"""Minimal Dagster orchestration for the weather ELT pipeline.

Wires the two stages into a dependency graph so the transform only runs after a
successful extract - the modern equivalent of the sentinel/_SUCCESS trigger used
by the production scrapers:

    extract_weather_op  ->  dbt_build_op

Run the UI with:

    dagster dev -f orchestration/dagster_pipeline.py

Then materialize the `weather_elt` job (or attach the provided daily schedule).
This is intentionally lightweight (ops shelling out to the extractor and the dbt
CLI) so it is easy to read; a production setup would use dagster-dbt assets.
"""

import subprocess
import sys
from pathlib import Path

from dagster import (
    Definitions,
    OpExecutionContext,
    ScheduleDefinition,
    job,
    op,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXTRACTOR = PROJECT_ROOT / "extract" / "extract_weather.py"
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt" / "weather"


def _run(context: OpExecutionContext, cmd: list[str], cwd: Path) -> None:
    context.log.info(f"$ {' '.join(cmd)}  (cwd={cwd})")
    result = subprocess.run(cmd, cwd=str(cwd), capture_output=True, text=True)
    if result.stdout:
        context.log.info(result.stdout)
    if result.stderr:
        context.log.info(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"Command failed ({result.returncode}): {' '.join(cmd)}")


@op
def extract_weather_op(context: OpExecutionContext) -> bool:
    _run(context, [sys.executable, str(EXTRACTOR), "--mode", "all"], cwd=PROJECT_ROOT)
    return True


@op
def dbt_build_op(context: OpExecutionContext, extract_ok: bool) -> None:
    if not extract_ok:
        raise RuntimeError("Extract stage did not succeed; skipping dbt build.")
    # profiles.yml lives in the project dir, so point --profiles-dir at it.
    _run(
        context,
        ["dbt", "build", "--profiles-dir", str(DBT_PROJECT_DIR)],
        cwd=DBT_PROJECT_DIR,
    )


@job
def weather_elt():
    dbt_build_op(extract_weather_op())


hourly_schedule = ScheduleDefinition(
    job=weather_elt,
    cron_schedule="0 * * * *",  # top of every hour
    execution_timezone="UTC",
)

defs = Definitions(jobs=[weather_elt], schedules=[hourly_schedule])
