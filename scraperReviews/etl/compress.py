"""ZSTD Parquet writer for review data."""

from __future__ import annotations

from pathlib import Path

import polars as pl


def write_parquet(
    df: pl.DataFrame, output_path: Path, compression_level: int = 9
) -> int:
    """Write `df` to ZSTD-compressed Parquet. Returns size in bytes."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(output_path, compression="zstd",
                     compression_level=compression_level)
    return output_path.stat().st_size
