"""Parametrized dlt pipeline — the unit of work the benchmark measures.

Everything here is **fully local**: the destination is a DuckDB *file* on
disk (no server, no container, no cloud account). One call to
:func:`run_pipeline` performs a single benchmarked load and returns a
:class:`RunResult` with timing/throughput metadata. The result is also
appended to a local ``benchmark_runs`` table in the same DuckDB file so the
pipeline is useful standalone, before Dagster/dbt exist.

Run it directly for a smoke test::

    python -m dlt_pipelines.pipeline
"""

from __future__ import annotations

import logging
import os
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import dlt
import duckdb
from dotenv import load_dotenv

from dlt_pipelines.synthetic_source import Complexity, RowWidth, synthetic_source

load_dotenv()
logger = logging.getLogger(__name__)

#: Table (in the benchmark DuckDB) that accumulates one row per pipeline run.
RUNS_TABLE = "benchmark_runs"


@dataclass(slots=True)
class RunResult:
    """Metadata + measurements for a single pipeline run.

    Attributes:
        run_id: Unique id for this run (also tags the resource samples).
        batch_size: The benchmarked variable — rows per chunk.
        row_width: Column-count variant ("narrow"/"medium"/"wide").
        complexity: JSON nesting variant ("simple"/"complex").
        total_rows: Rows requested for this run.
        rows_loaded: Rows actually loaded into DuckDB (sanity check).
        start_time: UTC ISO timestamp the load started.
        end_time: UTC ISO timestamp the load finished.
        duration_sec: Wall-clock load duration in seconds.
        throughput_rows_per_sec: ``rows_loaded / duration_sec``.
        repetition: Which repeat this is (1-based) within a sweep cell.
        dataset_name: dlt dataset (schema) the run wrote to.
    """

    run_id: str
    batch_size: int
    row_width: RowWidth
    complexity: Complexity
    total_rows: int
    rows_loaded: int
    start_time: str
    end_time: str
    duration_sec: float
    throughput_rows_per_sec: float
    repetition: int
    dataset_name: str


def _duckdb_path() -> Path:
    """Resolve the benchmark DuckDB path from the environment.

    Returns:
        Absolute path to the DuckDB file (parent dirs are created).
    """
    raw = os.getenv("DUCKDB_PATH", "data/duckdb/benchmark.duckdb")
    path = Path(raw).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _count_loaded_rows(db_path: Path, dataset: str) -> int:
    """Count rows in the loaded ``events`` table.

    Args:
        db_path: DuckDB file path.
        dataset: dlt dataset/schema name the run wrote to.

    Returns:
        Row count, or ``0`` if the table is absent.
    """
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        return con.execute(f'SELECT count(*) FROM "{dataset}"."events"').fetchone()[0]
    except duckdb.Error:
        logger.warning("Could not count rows in %s.events", dataset)
        return 0
    finally:
        con.close()


def _persist_run(db_path: Path, result: RunResult) -> None:
    """Append one :class:`RunResult` to the local ``benchmark_runs`` table.

    Uses a plain DuckDB connection (no extra infrastructure) so the table
    exists even when running the pipeline standalone.

    Args:
        db_path: DuckDB file path.
        result: The run metadata to persist.
    """
    con = duckdb.connect(str(db_path))
    try:
        con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {RUNS_TABLE} (
                run_id                   VARCHAR,
                batch_size               BIGINT,
                row_width                VARCHAR,
                complexity               VARCHAR,
                total_rows               BIGINT,
                rows_loaded              BIGINT,
                start_time               TIMESTAMP,
                end_time                 TIMESTAMP,
                duration_sec             DOUBLE,
                throughput_rows_per_sec  DOUBLE,
                repetition               INTEGER,
                dataset_name             VARCHAR
            )
            """
        )
        row = asdict(result)
        con.execute(
            f"INSERT INTO {RUNS_TABLE} VALUES "
            "($run_id,$batch_size,$row_width,$complexity,$total_rows,$rows_loaded,"
            "$start_time,$end_time,$duration_sec,$throughput_rows_per_sec,"
            "$repetition,$dataset_name)",
            row,
        )
    finally:
        con.close()
    logger.info("Persisted run %s to %s", result.run_id, RUNS_TABLE)


def run_pipeline(
    batch_size: int,
    total_rows: int,
    row_width: RowWidth = "medium",
    complexity: Complexity = "simple",
    repetition: int = 1,
    seed: int = 42,
) -> RunResult:
    """Run one benchmarked dlt load into local DuckDB and return its metrics.

    A fresh ``run_id`` and an isolated dlt dataset are used per run so
    concurrent table state never bleeds between runs. ``write_disposition``
    is ``replace`` (set on the resource) to guarantee a clean load each time.

    Args:
        batch_size: Rows per chunk — the experimental variable.
        total_rows: Rows to generate and load.
        row_width: Column-count variant.
        complexity: JSON nesting variant.
        repetition: 1-based repeat index within a sweep cell.
        seed: Determinism seed forwarded to the synthetic source.

    Returns:
        A populated :class:`RunResult`.
    """
    run_id = uuid.uuid4().hex
    db_path = _duckdb_path()
    # Unique dataset per run keeps loads isolated and replace-safe.
    dataset_name = f"bench_{run_id[:8]}"

    pipeline = dlt.pipeline(
        pipeline_name="batch_benchmark",
        destination=dlt.destinations.duckdb(credentials=str(db_path)),
        dataset_name=dataset_name,
        progress=None,
    )

    source = synthetic_source(
        total_rows=total_rows,
        batch_size=batch_size,
        row_width=row_width,
        complexity=complexity,
        seed=seed,
    )

    logger.info(
        "RUN %s | batch_size=%d total_rows=%d row_width=%s complexity=%s rep=%d",
        run_id,
        batch_size,
        total_rows,
        row_width,
        complexity,
        repetition,
    )

    start = datetime.now(timezone.utc)
    pipeline.run(source)
    end = datetime.now(timezone.utc)

    duration = (end - start).total_seconds()
    rows_loaded = _count_loaded_rows(db_path, dataset_name)
    throughput = rows_loaded / duration if duration > 0 else 0.0

    result = RunResult(
        run_id=run_id,
        batch_size=batch_size,
        row_width=row_width,
        complexity=complexity,
        total_rows=total_rows,
        rows_loaded=rows_loaded,
        start_time=start.isoformat(),
        end_time=end.isoformat(),
        duration_sec=duration,
        throughput_rows_per_sec=throughput,
        repetition=repetition,
        dataset_name=dataset_name,
    )
    _persist_run(db_path, result)

    logger.info(
        "DONE %s | %.1f rows/sec (%d rows in %.2fs)",
        run_id,
        throughput,
        rows_loaded,
        duration,
    )
    return result


def _defaults_from_env() -> dict[str, Any]:
    """Read standalone-run defaults from environment variables.

    Returns:
        Kwargs for :func:`run_pipeline`.
    """
    return {
        "total_rows": int(os.getenv("BENCHMARK_TOTAL_ROWS", "5000000")),
        "batch_size": int(os.getenv("BENCHMARK_DEFAULT_BATCH_SIZE", "10000")),
        "row_width": os.getenv("BENCHMARK_DEFAULT_ROW_WIDTH", "medium"),
        "complexity": os.getenv("BENCHMARK_DEFAULT_COMPLEXITY", "simple"),
        "seed": int(os.getenv("FAKER_SEED", "42")),
    }


if __name__ == "__main__":
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    cfg = _defaults_from_env()
    logger.info("Standalone smoke test with .env defaults: %s", cfg)
    outcome = run_pipeline(**cfg)
    logger.info("Result: %s", asdict(outcome))
