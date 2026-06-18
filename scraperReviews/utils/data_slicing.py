"""Split a large Parquet into <100MB chunks for GitHub upload (no LFS)."""

from __future__ import annotations

from pathlib import Path

import polars as pl


def slice_data(input_file: Path, output_dir: Path, parts: int = 2) -> None:
    """Split `input_file` into `parts` ZSTD-Parquet chunks under `output_dir`."""
    if not input_file.exists():
        print(f"[ERROR] Input file not found: {input_file}")
        return

    print(f"Reading {input_file.name}...")
    df = pl.read_parquet(input_file)
    total = len(df)
    per_part = total // parts

    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Splitting {total:,} rows into {parts} parts...")
    for i in range(parts):
        start = i * per_part
        end = (i + 1) * per_part if i < parts - 1 else total
        chunk = df.slice(start, end - start)
        out = output_dir / f"reviews_peru_part{i + 1}.parquet"
        chunk.write_parquet(out, compression="zstd", compression_level=9)
        size_mb = out.stat().st_size / (1024 * 1024)
        print(f"  [DONE] {out.name} ({size_mb:.2f} MB)")


if __name__ == "__main__":
    root = Path(__file__).resolve().parent.parent
    slice_data(
        root / "data_parquet" / "Peru" / "reviews_peru.parquet",
        root / "data_gh",
    )
