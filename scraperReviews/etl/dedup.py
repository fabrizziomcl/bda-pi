"""Deduplicate the raw reviews CSV by id_review."""

from __future__ import annotations

from pathlib import Path

import polars as pl


def load_and_deduplicate(csv_path: Path) -> tuple[pl.DataFrame, int]:
    """Read CSV, drop empty/duplicate `id_review`. Returns (df, raw_count)."""
    if not csv_path.exists():
        return pl.DataFrame(), 0
    try:
        df = pl.read_csv(
            str(csv_path), infer_schema_length=0, ignore_errors=True,
            encoding="utf8-lossy",
        )
    except Exception:
        return pl.DataFrame(), 0

    if df.is_empty():
        return pl.DataFrame(), 0

    raw_count = len(df)
    if "id_review" in df.columns:
        df = df.filter(pl.col("id_review").is_not_null() & (pl.col("id_review") != ""))
        df = df.unique(subset=["id_review"])
    return df, raw_count
