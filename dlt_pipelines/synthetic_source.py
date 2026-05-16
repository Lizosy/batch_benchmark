"""Faker-based synthetic event source for the batch-size benchmark.

The generator is intentionally *deterministic* (seeded Faker + a seeded
``random.Random``) so that two runs with the same parameters produce the same
rows. That removes data variability as a confound when comparing batch sizes.

Two orthogonal knobs shape each row:

* ``row_width``  -> how many columns ("narrow" 5, "medium" 15, "wide" 50)
* ``complexity`` -> "simple" (shallow JSON) vs "complex" (deeply nested JSON)

The module exposes both a plain Python generator (``generate_records``) for
unit testing and a ``dlt`` resource (``synthetic_source``) whose *batch size*
is the experimental variable: records are yielded in chunks of ``batch_size``,
which is the granularity dlt normalizes/loads at.
"""

from __future__ import annotations

import logging
import random
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Iterator, Literal, get_args

import dlt
from faker import Faker

logger = logging.getLogger(__name__)

# --- Type aliases ---------------------------------------------------------
RowWidth = Literal["narrow", "medium", "wide"]
Complexity = Literal["simple", "complex"]

#: Column count target for each row-width variant.
WIDTH_COLUMN_COUNT: dict[RowWidth, int] = {"narrow": 5, "medium": 15, "wide": 50}

#: Fixed categorical domain for ``event_name`` (exactly 10 distinct values).
EVENT_NAMES: tuple[str, ...] = (
    "page_view",
    "click",
    "add_to_cart",
    "checkout_start",
    "purchase",
    "signup",
    "login",
    "logout",
    "search",
    "share",
)

#: Epoch all timestamps are offset from, so runs are reproducible.
_EPOCH = datetime(2025, 1, 1, tzinfo=timezone.utc)

#: The five core scalar columns present in every row (== "narrow").
_CORE_COLUMNS = ("id", "user_id", "event_name", "event_timestamp", "amount")


def _build_properties(rng: random.Random, complexity: Complexity) -> dict[str, Any]:
    """Build the ``properties`` JSON blob (5-10 fields).

    Args:
        rng: Seeded RNG so the structure is reproducible.
        complexity: ``"simple"`` yields a flat dict; ``"complex"`` nests a
            ``device`` sub-object and a list of tag dicts.

    Returns:
        A JSON-serializable dict.
    """
    props: dict[str, Any] = {
        "session_id": str(uuid.UUID(int=rng.getrandbits(128))),
        "referrer": rng.choice(["google", "direct", "email", "social", "ads"]),
        "page": f"/p/{rng.randint(1, 500)}",
        "ab_variant": rng.choice(["A", "B", "control"]),
        "is_returning": rng.random() > 0.5,
    }
    if complexity == "complex":
        props["device"] = {
            "os": rng.choice(["ios", "android", "windows", "macos", "linux"]),
            "browser": rng.choice(["chrome", "safari", "firefox", "edge"]),
            "screen": {"w": rng.choice([360, 768, 1280, 1920]), "h": rng.choice([640, 1024, 1080])},
        }
        props["tags"] = [
            {"key": f"tag_{i}", "weight": round(rng.random(), 4)}
            for i in range(rng.randint(2, 5))
        ]
    else:
        props["device_os"] = rng.choice(["ios", "android", "windows", "macos", "linux"])
        props["browser"] = rng.choice(["chrome", "safari", "firefox", "edge"])
    return props


def _build_metadata(rng: random.Random, complexity: Complexity) -> dict[str, Any]:
    """Build the ``metadata`` JSON blob with variable depth.

    Args:
        rng: Seeded RNG.
        complexity: ``"complex"`` produces a recursively nested structure;
            ``"simple"`` produces a single flat level.

    Returns:
        A JSON-serializable dict.
    """
    base: dict[str, Any] = {
        "source": rng.choice(["batch", "stream", "backfill"]),
        "schema_version": rng.randint(1, 4),
        "ingested_via": "synthetic",
    }
    if complexity == "complex":
        depth = rng.randint(2, 4)
        node: dict[str, Any] = {"leaf": rng.random()}
        for level in range(depth):
            node = {f"level_{level}": node, "n": rng.randint(0, 999)}
        base["nested"] = node
    return base


def make_record(
    record_id: int,
    faker: Faker,
    rng: random.Random,
    row_width: RowWidth,
    complexity: Complexity,
) -> dict[str, Any]:
    """Create a single synthetic event record.

    The number of keys in the returned dict equals ``WIDTH_COLUMN_COUNT``
    for the given ``row_width``.

    Args:
        record_id: Sequential integer primary key.
        faker: A seeded Faker instance.
        rng: A seeded ``random.Random`` (kept separate from Faker for speed
            on the hot path).
        row_width: Controls how many columns the record has.
        complexity: Controls JSON nesting depth in ``properties``/``metadata``.

    Returns:
        A JSON-serializable record dict.
    """
    record: dict[str, Any] = {
        "id": record_id,
        "user_id": str(uuid.UUID(int=rng.getrandbits(128))),
        "event_name": EVENT_NAMES[record_id % len(EVENT_NAMES)],
        "event_timestamp": _EPOCH + timedelta(seconds=record_id, milliseconds=rng.randint(0, 999)),
        "amount": Decimal(str(round(rng.uniform(0.0, 5000.0), 2))),
    }

    target = WIDTH_COLUMN_COUNT[row_width]
    if row_width == "narrow":
        return record  # exactly the 5 core columns

    # "medium" and "wide" both carry a nested properties blob.
    record["properties"] = _build_properties(rng, complexity)

    if row_width == "wide":
        record["metadata"] = _build_metadata(rng, complexity)

    # Pad with deterministic scalar attribute columns up to the target width.
    filler = target - len(record)
    for i in range(filler):
        record[f"attr_{i:02d}"] = round(rng.uniform(-1000.0, 1000.0), 3)

    return record


def generate_records(
    total_rows: int,
    row_width: RowWidth = "medium",
    complexity: Complexity = "simple",
    seed: int = 42,
) -> Iterator[dict[str, Any]]:
    """Yield ``total_rows`` synthetic records one at a time (pure generator).

    This function has no dlt dependency so it can be unit-tested in isolation.

    Args:
        total_rows: Number of records to produce.
        row_width: ``"narrow"`` | ``"medium"`` | ``"wide"``.
        complexity: ``"simple"`` | ``"complex"``.
        seed: Seed applied to both Faker and the RNG for reproducibility.

    Yields:
        Record dicts.

    Raises:
        ValueError: If ``total_rows`` is negative, or an unknown
            ``row_width`` / ``complexity`` is supplied.
    """
    if total_rows < 0:
        raise ValueError(f"total_rows must be >= 0, got {total_rows}")
    if row_width not in get_args(RowWidth):
        raise ValueError(f"row_width must be one of {get_args(RowWidth)}, got {row_width!r}")
    if complexity not in get_args(Complexity):
        raise ValueError(f"complexity must be one of {get_args(Complexity)}, got {complexity!r}")

    faker = Faker()
    Faker.seed(seed)
    rng = random.Random(seed)

    logger.info(
        "Generating %d rows (row_width=%s, complexity=%s, seed=%d)",
        total_rows,
        row_width,
        complexity,
        seed,
    )
    for record_id in range(total_rows):
        yield make_record(record_id, faker, rng, row_width, complexity)


@dlt.source(name="synthetic")
def synthetic_source(
    total_rows: int,
    batch_size: int,
    row_width: RowWidth = "medium",
    complexity: Complexity = "simple",
    seed: int = 42,
):
    """dlt source wrapping :func:`generate_records`, chunked by ``batch_size``.

    ``batch_size`` is *the experimental variable* of this project. The resource
    yields lists of length ``batch_size`` (the final chunk may be shorter);
    dlt treats each yielded list as a batch it normalizes and loads together,
    so this knob directly controls the pipeline's batching granularity.

    Args:
        total_rows: Total records to emit across all batches.
        batch_size: Records per yielded chunk (the benchmarked variable).
        row_width: Column-count variant.
        complexity: JSON nesting variant.
        seed: Determinism seed.

    Returns:
        A dlt resource named ``events``.

    Raises:
        ValueError: If ``batch_size`` is not positive.
    """
    if batch_size <= 0:
        raise ValueError(f"batch_size must be > 0, got {batch_size}")

    @dlt.resource(name="events", write_disposition="replace")
    def events() -> Iterator[list[dict[str, Any]]]:
        batch: list[dict[str, Any]] = []
        for record in generate_records(total_rows, row_width, complexity, seed):
            batch.append(record)
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    return events


if __name__ == "__main__":
    # Tiny manual sanity check:  python -m dlt_pipelines.synthetic_source
    logging.basicConfig(level=logging.INFO)
    for width in get_args(RowWidth):
        sample = next(generate_records(1, row_width=width, complexity="complex"))
        logger.info("row_width=%s -> %d columns: %s", width, len(sample), list(sample))
