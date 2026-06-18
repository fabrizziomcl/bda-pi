"""Reviews ETL orchestrator: raw CSV → deduplicated Parquet + report."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from etl.compress import write_parquet
from etl.dedup import load_and_deduplicate
from etl.optimize import optimize_schema
from etl.report import format_bytes


def _load_state(state_path: Path) -> dict:
    if not state_path.exists():
        return {}
    try:
        return json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _save_state(state_path: Path, payload: dict) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = state_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp.replace(state_path)


def run_pipeline(input_csv: Path, output_dir: Path, *, incremental: bool = False) -> None:
    """Dedup, optimize, and write the raw reviews CSV to Parquet.

    `incremental=True` short-circuits the run when the input has not grown
    since the previous successful run (size+mtime watermark).
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    state_path = output_dir / "etl_state.json"

    print("=" * 80)
    print("REVIEWS ETL PIPELINE")
    print(f"  Input:        {input_csv.resolve()}")
    print(f"  Output:       {output_dir.resolve()}")
    print(f"  Incremental:  {incremental}")
    print("=" * 80)

    if not input_csv.exists():
        print(f"  [ERROR] Input file not found: {input_csv}")
        return

    if incremental and _is_unchanged(input_csv, _load_state(state_path)):
        print("  [SKIP] Input unchanged since last run.")
        return

    overall_start = time.time()
    input_size = input_csv.stat().st_size

    print("\n  [1/3] Loading and deduplicating reviews...")
    df, raw_count = load_and_deduplicate(input_csv)
    if df.is_empty():
        print("  [ERROR] No valid reviews found after deduplication.")
        return
    unique_count = len(df)
    print(f"         Raw records:    {raw_count:,}")
    print(f"         Unique reviews: {unique_count:,}")
    print(f"         Duplicates removed: {raw_count - unique_count:,}")

    print("\n  [2/3] Optimizing schema...")
    df = optimize_schema(df)

    print("\n  [3/3] Writing outputs...")
    peru_dir = output_dir / "Peru"
    peru_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = peru_dir / "reviews_peru.parquet"
    csv_path = peru_dir / "reviews_peru.csv"
    parquet_size = write_parquet(df, parquet_path)
    df.write_csv(csv_path)
    csv_output_size = csv_path.stat().st_size
    print(f"         Parquet: {parquet_path.name} ({format_bytes(parquet_size)})")
    print(f"         CSV:     {csv_path.name} ({format_bytes(csv_output_size)})")

    unique_places = df.select("place_id").n_unique() if "place_id" in df.columns else 0
    elapsed = time.time() - overall_start
    reduction = (1 - parquet_size / input_size) * 100 if input_size else 0.0

    _print_summary(
        raw_count, unique_count, unique_places,
        input_size, csv_output_size, parquet_size,
        reduction, elapsed,
    )

    _save_report(
        output_dir, raw_count, unique_count, unique_places,
        input_size, csv_output_size, parquet_size, reduction, elapsed,
    )

    _save_state(state_path, {
        "input_size": input_csv.stat().st_size,
        "input_mtime": int(input_csv.stat().st_mtime),
        "input_mtime_iso": time.strftime(
            "%Y-%m-%dT%H:%M:%S", time.localtime(input_csv.stat().st_mtime)
        ),
        "unique_reviews": unique_count,
        "ran_at_iso": time.strftime("%Y-%m-%dT%H:%M:%S"),
    })


def _is_unchanged(input_csv: Path, state: dict) -> bool:
    try:
        st = input_csv.stat()
    except OSError:
        return False
    return (
        state.get("input_size") == st.st_size
        and state.get("input_mtime") == int(st.st_mtime)
    )


def _print_summary(
    raw_count, unique_count, unique_places,
    input_size, csv_output_size, parquet_size,
    reduction, elapsed,
) -> None:
    print("\n" + "=" * 80)
    print("SUMMARY")
    print(f"  Total raw records:          {raw_count:,}")
    print(f"  Unique reviews:             {unique_count:,}")
    print(f"  Unique places with reviews: {unique_places:,}")
    print(f"  Duplicates removed:         {raw_count - unique_count:,}")
    print(f"  Input CSV size:             {format_bytes(input_size)}")
    print(f"  Output CSV size:            {format_bytes(csv_output_size)}")
    print(f"  Output Parquet size:        {format_bytes(parquet_size)}")
    print(f"  Size reduction:             {reduction:.2f}%")
    print(f"  Total time:                 {elapsed:.2f}s")
    print("=" * 80)


def _save_report(
    output_dir, raw_count, unique_count, unique_places,
    input_size, csv_output_size, parquet_size, reduction, elapsed,
) -> None:
    payload = {
        "total_raw_records": raw_count,
        "unique_reviews": unique_count,
        "unique_places_with_reviews": unique_places,
        "duplicates_removed": raw_count - unique_count,
        "input_csv_size_bytes": input_size,
        "input_csv_size_formatted": format_bytes(input_size),
        "output_csv_size_bytes": csv_output_size,
        "output_csv_size_formatted": format_bytes(csv_output_size),
        "output_parquet_size_bytes": parquet_size,
        "output_parquet_size_formatted": format_bytes(parquet_size),
        "size_reduction_percentage": round(reduction, 2),
        "total_time_seconds": round(elapsed, 2),
    }
    report_path = output_dir / "etl_report.json"
    report_path.write_text(
        json.dumps(payload, indent=4, ensure_ascii=False), encoding="utf-8"
    )
    print(f"  [INFO] Report saved to {report_path.resolve()}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Reviews ETL pipeline")
    parser.add_argument(
        "--input", type=str, default="data/output/reviews_raw.csv",
        help="Path to the raw reviews CSV.",
    )
    parser.add_argument(
        "--output-dir", type=str, default="data_parquet",
        help="Path to the output directory.",
    )
    parser.add_argument(
        "--incremental", action="store_true",
        help="Skip the run if the input CSV is unchanged since last successful run.",
    )
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent.parent
    run_pipeline(
        project_root / args.input,
        project_root / args.output_dir,
        incremental=args.incremental,
    )


if __name__ == "__main__":
    main()
