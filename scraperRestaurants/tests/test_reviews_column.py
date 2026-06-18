"""Regression tests guarding the removed `reviews` column.

Google's tbm=map response stopped returning per-place review counts; an
empirical sweep over 407,436 rows showed 0 populated values, so the column
was dropped from the schema. These tests guard against silent regressions:
the crawler must not emit it, and any legacy on-disk CSVs that still carry
the column must remain empty.
"""

from __future__ import annotations

import random
from pathlib import Path

import polars as pl
import pytest

from crawler.placesCrawlerV2 import _extract_place, save_to_csv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"


def _synthetic_result(place_id: str = "ChIJTEST", title: str = "Test Place") -> list:
    """Build a sparse result list mirroring data[64][i][1]."""
    result: list = [None] * 260
    result[4] = [None] * 8
    result[4][7] = 4.5
    result[7] = ["https://example.com", "example.com"]
    result[9] = [None, None, -12.04, -77.03]
    result[11] = title
    result[13] = ["Restaurante"]
    result[39] = "Av. Test 123, Lima"
    result[78] = place_id
    return result


def test_extract_place_does_not_emit_reviews_field() -> None:
    place = _extract_place(_synthetic_result(), query="test query")
    assert place is not None
    assert "reviews" not in place


def test_save_to_csv_header_does_not_include_reviews(tmp_path) -> None:
    out = tmp_path / "out.csv"
    save_to_csv([{"id": "x", "url_place": "u", "title": "t"}], str(out))
    header = out.read_text(encoding="utf-8").splitlines()[0]
    assert "reviews" not in header.split(",")


@pytest.mark.skipif(not DATA_DIR.exists(), reason="No data/ to scan")
def test_on_disk_csvs_have_empty_reviews_column() -> None:
    """Up to 200 random CSVs: assert <1% of rows have a populated `reviews`."""
    csvs = list(DATA_DIR.rglob("*.csv"))
    if not csvs:
        pytest.skip("No CSV files under data/")

    random.seed(42)
    sample = random.sample(csvs, min(200, len(csvs)))

    total_rows = 0
    populated = 0
    for path in sample:
        try:
            df = pl.read_csv(str(path), infer_schema_length=0, ignore_errors=True)
        except Exception:
            continue
        if df.is_empty() or "reviews" not in df.columns:
            continue
        total_rows += len(df)
        populated += int(
            (df["reviews"].is_not_null() & (df["reviews"] != "")).sum()
        )

    if total_rows == 0:
        pytest.skip("No rows contained a reviews column")

    ratio = populated / total_rows
    assert ratio < 0.01, (
        f"`reviews` populated at {ratio:.4%} ({populated:,}/{total_rows:,})"
    )
