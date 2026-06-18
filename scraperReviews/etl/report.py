"""Human-readable formatters for review ETL output."""

from __future__ import annotations


def format_bytes(size_bytes: int) -> str:
    """Convert a byte count to a KB/MB/GB string."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    if size_bytes < 1024 ** 2:
        return f"{size_bytes / 1024:.1f} KB"
    if size_bytes < 1024 ** 3:
        return f"{size_bytes / (1024 ** 2):.2f} MB"
    return f"{size_bytes / (1024 ** 3):.2f} GB"


def processing_summary(
    label: str, raw_rows: int, unique_rows: int,
    csv_bytes: int, parquet_bytes: int,
) -> str:
    """One-line summary for a processing step."""
    ratio = (1 - parquet_bytes / csv_bytes) * 100 if csv_bytes else 0.0
    return (
        f"  {label:<25} | "
        f"Raw: {raw_rows:>10,} -> Unique: {unique_rows:>10,} | "
        f"CSV: {format_bytes(csv_bytes):>10} -> "
        f"Parquet: {format_bytes(parquet_bytes):>10} | "
        f"Reduction: {ratio:5.1f}%"
    )
