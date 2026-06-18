"""Type optimization for review Parquet output."""

from __future__ import annotations

import polars as pl

# Categorical only helps when values repeat; skip the cast when the column
# is mostly unique to avoid a dictionary as large as the data itself.
CATEGORICAL_MAX_UNIQUE_RATIO = 0.5


def optimize_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Cast rating→Float32, n_review_user→Int32, place_id/username→Categorical."""
    cols = set(df.columns)
    expressions: list[pl.Expr] = []

    if "rating" in cols:
        expressions.append(pl.col("rating").cast(pl.Float32, strict=False))

    if "n_review_user" in cols:
        expressions.append(
            pl.when(pl.col("n_review_user") == "")
            .then(None)
            .otherwise(pl.col("n_review_user"))
            .cast(pl.Int32, strict=False)
            .alias("n_review_user")
        )

    for col in ("place_id", "username"):
        expr = _safe_categorical(df, col)
        if expr is not None:
            expressions.append(expr)

    return df.with_columns(expressions) if expressions else df


def _safe_categorical(df: pl.DataFrame, col: str) -> pl.Expr | None:
    if col not in df.columns:
        return None
    n = len(df)
    if n == 0:
        return pl.col(col).cast(pl.Categorical)
    if df[col].n_unique() / n > CATEGORICAL_MAX_UNIQUE_RATIO:
        return None
    return pl.col(col).cast(pl.Categorical)
