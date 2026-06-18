"""Sample N places from the full input CSV for quick pipeline tests."""

from __future__ import annotations

import argparse
import random
from pathlib import Path

import polars as pl


def create_test_sample(source: Path, output: Path, n_samples: int = 50) -> None:
    """Write a random sample to `output`. Prefers places with non-empty `reviews`."""
    df = pl.read_csv(str(source), infer_schema_length=0, ignore_errors=True)
    df = df.filter(pl.col("url_place").is_not_null() & (pl.col("url_place") != ""))

    if "reviews" in df.columns:
        with_reviews = df.filter(
            pl.col("reviews").is_not_null() & (pl.col("reviews") != "")
        )
        if len(with_reviews) >= n_samples:
            df = with_reviews

    if len(df) > n_samples:
        indices = random.sample(range(len(df)), n_samples)
        df = df[indices]

    output.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(output)
    print(f"Test sample: {output}")
    print(f"  Places: {len(df)}")
    print(f"  Columns: {df.columns}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Create a test environment.")
    parser.add_argument("--source", type=str, default="data/input/places_peru.csv")
    parser.add_argument("--sample", type=int, default=50)
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent.parent
    source = project_root / args.source
    if not source.exists():
        print(f"Error: source not found: {source}")
        return

    output = project_root / "data" / "test" / "sample_places.csv"
    create_test_sample(source, output, args.sample)


if __name__ == "__main__":
    main()
