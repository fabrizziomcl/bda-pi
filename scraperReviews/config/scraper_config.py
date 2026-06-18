"""Tunables for the reviews scraper. One source of truth for magic numbers."""

from __future__ import annotations

from typing import Final

# ── Orchestrator ───────────────────────────────────────────────────────────
DEFAULT_WORKERS: Final[int] = 8

# Per-place review cap. 0 / None → unlimited (target 100% recall on places
# with >15k reviews; combine with a higher PLACE_TIMEOUT).
MAX_REVIEWS_PER_PLACE: Final[int] = 15_000
DEFAULT_SORT_BY: Final[str] = "newest"

# ── Rate limiting ──────────────────────────────────────────────────────────
DELAY_BETWEEN_PLACES_MIN: Final[float] = 2.0
DELAY_BETWEEN_PLACES_MAX: Final[float] = 5.0
WORKER_RETRY_MAX: Final[int] = 2
WORKER_RETRY_BACKOFF_BASE: Final[float] = 5.0
PLACE_TIMEOUT: Final[float] = 180.0

# ── Browser ────────────────────────────────────────────────────────────────
VIEWPORT: Final[dict] = {"width": 1920, "height": 1080}

# ── Output ─────────────────────────────────────────────────────────────────
RAW_OUTPUT_FILENAME: Final[str] = "reviews_raw.csv"
COMPLETED_PLACES_FILENAME: Final[str] = "completed_places.txt"

# `n_photo_user` is reserved: the scraper does not currently extract photo
# counts. The column stays in the schema so existing Bronze/Silver datasets
# remain valid; populate it when the extractor gains the capability.
REVIEW_HEADER: Final[list[str]] = [
    "place_id", "id_review", "caption", "relative_date", "review_date",
    "retrieval_date", "rating", "username", "n_review_user",
    "n_photo_user", "url_user", "url_source",
]

# ── Paths (relative to project root) ───────────────────────────────────────
DEFAULT_INPUT_FILE: Final[str] = "data/input/places_peru.csv"
DEFAULT_OUTPUT_DIR: Final[str] = "data/output"
DEFAULT_PARQUET_DIR: Final[str] = "data_parquet"
DEFAULT_TEST_DIR: Final[str] = "data/test"

# ── ETL ────────────────────────────────────────────────────────────────────
PARQUET_COMPRESSION: Final[str] = "zstd"
PARQUET_COMPRESSION_LEVEL: Final[int] = 9
DEDUP_COLUMN: Final[str] = "id_review"
