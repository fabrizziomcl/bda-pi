"""Tests for reviews ETL + the Fase-2 pre-filter in orchestrator.load_places."""

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


def test_dedup_by_id_review(tmp_path) -> None:
    p = tmp_path / "raw.csv"
    _write_csv(p, [
        {"place_id": "A", "id_review": "r1", "caption": "ok"},
        {"place_id": "A", "id_review": "r1", "caption": "dup"},
        {"place_id": "A", "id_review": "r2", "caption": "ok"},
    ])
    df, raw = load_and_deduplicate(p)
    assert raw == 3
    assert len(df) == 2


def test_dedup_skips_empty_id_review(tmp_path) -> None:
    p = tmp_path / "raw.csv"
    _write_csv(p, [
        {"place_id": "A", "id_review": "r1"},
        {"place_id": "A", "id_review": ""},
        {"place_id": "A", "id_review": "r2"},
    ])
    df, _ = load_and_deduplicate(p)
    assert len(df) == 2


def test_dedup_missing_file_returns_empty(tmp_path) -> None:
    df, raw = load_and_deduplicate(tmp_path / "nope.csv")
    assert raw == 0
    assert df.is_empty()


# ──────────────── optimize ────────────────


def test_optimize_rating_to_float() -> None:
    df = pl.DataFrame({
        "place_id": ["A", "B"],
        "id_review": ["r1", "r2"],
        "rating": ["5", "4"],
    })
    out = optimize_schema(df)
    assert out["rating"].dtype == pl.Float32
    assert out["rating"].to_list() == [pytest.approx(5.0), pytest.approx(4.0)]


def test_optimize_handles_empty_n_review_user() -> None:
    df = pl.DataFrame({
        "place_id": ["A"],
        "n_review_user": [""],
    })
    out = optimize_schema(df)
    assert out["n_review_user"].dtype == pl.Int32


# ──────────────── compress roundtrip ────────────────


def test_write_parquet_roundtrip(tmp_path) -> None:
    df = pl.DataFrame({
        "place_id": ["A", "B"],
        "id_review": ["r1", "r2"],
        "rating": [5.0, 4.0],
    })
    out = tmp_path / "out.parquet"
    assert write_parquet(df, out, compression_level=1) > 0
    rt = pl.read_parquet(out)
    assert len(rt) == 2


# ──────────────── pre-filter (orchestrator.load_places) ────────────────


def test_load_places_works_without_reviews_column(tmp_path) -> None:
    """Fase-1 CSVs no longer carry `reviews`; load_places must not pre-filter."""
    from orchestrator import load_places
    p = tmp_path / "places.csv"
    _write_csv(p, [
        {"id": "A", "url_place": "https://maps.google.com/?q=A"},
        {"id": "B", "url_place": "https://maps.google.com/?q=B"},
        {"id": "C", "url_place": ""},
    ])
    places = load_places(p)
    assert {p["place_id"] for p in places} == {"A", "B"}


def test_load_places_with_legacy_reviews_column(tmp_path) -> None:
    """Legacy CSV with populated `reviews` triggers the 0-review filter."""
    from orchestrator import load_places
    p = tmp_path / "places.csv"
    _write_csv(p, [
        {"id": "A", "url_place": "https://x/A", "reviews": "10"},
        {"id": "B", "url_place": "https://x/B", "reviews": "0"},
        {"id": "C", "url_place": "https://x/C", "reviews": "5"},
    ])
    ids = {p["place_id"] for p in load_places(p)}
    assert "B" not in ids
    assert {"A", "C"}.issubset(ids)
