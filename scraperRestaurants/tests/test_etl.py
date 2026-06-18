"""Tests for the mapScraper ETL: dedup, optimize, write_parquet roundtrip."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from etl.compress import write_parquet
from etl.dedup import load_and_deduplicate
from etl.optimize import optimize_schema


def _write_csv(path: Path, rows: list[dict]) -> None:
    cols = list(rows[0].keys()) if rows else []
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write(",".join(cols) + "\n")
        for r in rows:
            f.write(",".join(str(r.get(c, "")) for c in cols) + "\n")


# ──────────────── dedup ────────────────


def test_dedup_removes_duplicate_place_ids_within_one_file(tmp_path) -> None:
    p = tmp_path / "f1.csv"
    _write_csv(p, [
        {"id": "A", "title": "Place A"},
        {"id": "A", "title": "Place A duplicate"},
        {"id": "B", "title": "Place B"},
    ])
    df, raw = load_and_deduplicate([p])
    assert raw == 3
    assert len(df) == 2
    assert set(df["id"].to_list()) == {"A", "B"}


def test_dedup_across_multiple_files(tmp_path) -> None:
    p1 = tmp_path / "f1.csv"
    p2 = tmp_path / "f2.csv"
    _write_csv(p1, [{"id": "A", "title": "Lima"}])
    _write_csv(p2, [{"id": "A", "title": "Lima"}, {"id": "B", "title": "Callao"}])
    df, raw = load_and_deduplicate([p1, p2])
    assert raw == 3
    assert len(df) == 2


def test_dedup_drops_rows_with_empty_id(tmp_path) -> None:
    p = tmp_path / "f1.csv"
    _write_csv(p, [
        {"id": "A", "title": "good"},
        {"id": "",  "title": "blank id"},
        {"id": "B", "title": "good"},
    ])
    df, _ = load_and_deduplicate([p])
    assert len(df) == 2
    assert "" not in df["id"].to_list()


def test_dedup_empty_input_returns_empty() -> None:
    df, raw = load_and_deduplicate([])
    assert raw == 0
    assert df.is_empty()


# ──────────────── optimize ────────────────


def test_optimize_casts_stars_to_float() -> None:
    df = pl.DataFrame({
        "id": ["A", "B"],
        "stars": ["4.5", "3.2"],
        "category": ["x", "y"],
    })
    out = optimize_schema(df)
    assert out["stars"].dtype == pl.Float32
    assert out["stars"].to_list() == [pytest.approx(4.5), pytest.approx(3.2)]


def test_optimize_legacy_reviews_column_still_handled() -> None:
    df = pl.DataFrame({
        "id": ["A", "B"],
        "stars": ["4.5", "3.2"],
        "reviews": ["123", ""],
    })
    out = optimize_schema(df)
    assert out["reviews"].dtype == pl.Int32


def test_optimize_skips_missing_columns() -> None:
    df = pl.DataFrame({"id": ["A"], "title": ["x"]})
    out = optimize_schema(df)
    assert out.columns == ["id", "title"]


# ──────────────── compress roundtrip ────────────────


def test_write_parquet_is_readable_back(tmp_path) -> None:
    df = pl.DataFrame({
        "id": ["A", "B", "C"],
        "stars": [4.5, 3.2, 5.0],
    })
    out = tmp_path / "out.parquet"
    assert write_parquet(df, out, compression_level=1) > 0
    rt = pl.read_parquet(out)
    assert len(rt) == 3
    assert set(rt["id"].to_list()) == {"A", "B", "C"}
