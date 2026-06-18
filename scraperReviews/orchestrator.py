"""Parallel review scraping orchestrator — single browser, N async workers."""

from __future__ import annotations

import argparse
import asyncio
import csv
import sys
import time
from pathlib import Path

import polars as pl
from playwright.async_api import async_playwright
from tqdm import tqdm

from config.scraper_config import (
    COMPLETED_PLACES_FILENAME,
    DEFAULT_INPUT_FILE,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_PARQUET_DIR,
    DEFAULT_WORKERS,
    MAX_REVIEWS_PER_PLACE,
    RAW_OUTPUT_FILENAME,
    REVIEW_HEADER,
)
from logging_utils import setup_logger
from worker import TERMINAL_REASONS, ReviewWorker, WorkerContext

log = setup_logger("orchestrator", log_file="orchestrator.log")


# ──────────────── I/O ────────────────


def load_places(input_path: Path) -> list[dict]:
    """Read places CSV; apply legacy 0-review pre-filter when applicable."""
    df = pl.read_csv(
        str(input_path), infer_schema_length=0, ignore_errors=True,
        encoding="utf8-lossy",
    )
    missing = {"id", "url_place"} - set(df.columns)
    if missing:
        log.error("input CSV missing columns %s; got %s", missing, df.columns)
        sys.exit(1)

    df = df.filter(pl.col("url_place").is_not_null() & (pl.col("url_place") != ""))
    df = _apply_legacy_reviews_filter(df)

    return [
        {"place_id": r["id"], "url": r["url_place"], "title": r.get("title", "")}
        for r in df.iter_rows(named=True)
    ]


def _apply_legacy_reviews_filter(df: pl.DataFrame) -> pl.DataFrame:
    """Drop reviews=0 rows IFF the column is sufficiently populated."""
    if "reviews" not in df.columns:
        log.info("no `reviews` column; attempting all %d places", len(df))
        return df

    populated = df.filter(pl.col("reviews").is_not_null() & (pl.col("reviews") != ""))
    coverage = len(populated) / max(len(df), 1)
    if coverage <= 0.1:
        log.info(
            "`reviews` column coverage %.1f%% < 10%%; skipping pre-filter",
            coverage * 100,
        )
        return df

    before = len(df)
    df = df.filter(
        pl.col("reviews").is_null()
        | (pl.col("reviews") == "")
        | (pl.col("reviews") != "0")
    )
    skipped = before - len(df)
    if skipped:
        log.info("pre-filtered %d places with 0 reviews (coverage %.1f%%)",
                 skipped, coverage * 100)
    return df


def load_completed_records(completed_path: Path) -> dict[str, dict]:
    """Parse completed_places.txt (new+legacy formats). Last record per id wins."""
    out: dict[str, dict] = {}
    if not completed_path.exists():
        return out
    for raw in completed_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue
        out[line.split(",", 1)[0]] = _parse_completed_line(line)
    return out


def _parse_completed_line(line: str) -> dict:
    parts = line.split(",", 3)
    if len(parts) == 1:
        return {"place_id": parts[0], "scraped": 0, "reason": "ok", "ts": ""}
    try:
        scraped = int(parts[1])
    except ValueError:
        scraped = 0
    return {
        "place_id": parts[0],
        "scraped": scraped,
        "reason": parts[2] if len(parts) > 2 else "ok",
        "ts": parts[3] if len(parts) > 3 else "",
    }


def select_remaining(
    all_places: list[dict],
    completed: dict[str, dict],
    retry_non_terminal: bool,
) -> list[dict]:
    """Pick places that need scraping. Non-terminal reasons can opt-in to retry."""
    out: list[dict] = []
    for p in all_places:
        rec = completed.get(p["place_id"])
        if rec is None or rec["reason"] not in TERMINAL_REASONS and retry_non_terminal:
            out.append(p)
    return out


def init_output_csv(output_path: Path) -> None:
    if output_path.exists():
        return
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8", newline="\n") as f:
        csv.writer(f, quoting=csv.QUOTE_MINIMAL).writerow(REVIEW_HEADER)


# ──────────────── orchestration ────────────────


async def _worker_loop(
    worker: ReviewWorker, queue: asyncio.Queue, pbar: tqdm, stats: dict
) -> None:
    while True:
        try:
            place = queue.get_nowait()
        except asyncio.QueueEmpty:
            break
        count = await worker.scrape_place(place["place_id"], place["url"])
        if count >= 0:
            stats["reviews"] += count
        else:
            stats["errors"] += 1
        pbar.set_postfix(
            reviews=f"{stats['reviews']:,}", errors=stats["errors"], refresh=True
        )
        pbar.update(1)
        await worker.add_delay()
    await worker.shutdown()


async def run_orchestrator(
    input_path: Path,
    output_dir: Path,
    parquet_dir: Path,
    n_workers: int,
    max_reviews: int,
    debug: bool = False,
    skip_etl: bool = False,
    retry_non_terminal: bool = False,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_csv = output_dir / RAW_OUTPUT_FILENAME
    completed_path = output_dir / COMPLETED_PLACES_FILENAME

    all_places = load_places(input_path)
    completed = load_completed_records(completed_path)
    remaining = select_remaining(all_places, completed, retry_non_terminal)

    _print_banner(
        input_path, output_csv, n_workers, max_reviews,
        all_places, completed, remaining, retry_non_terminal,
    )

    if not remaining:
        log.info("all places already scraped")
        if not skip_etl:
            _run_etl(output_csv, parquet_dir)
        return

    init_output_csv(output_csv)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=not debug,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu",
            ],
        )
        ctx = WorkerContext(
            browser=browser,
            output_path=output_csv,
            completed_path=completed_path,
            csv_lock=asyncio.Lock(),
            completed_lock=asyncio.Lock(),
            max_reviews=max_reviews,
            debug=debug,
        )

        queue: asyncio.Queue = asyncio.Queue()
        for place in remaining:
            queue.put_nowait(place)

        workers = [ReviewWorker(i, ctx) for i in range(n_workers)]
        stats = {"reviews": 0, "errors": 0}
        started = time.time()

        pbar = tqdm(
            total=len(remaining), desc="Scraping reviews", unit="place",
            miniters=1, dynamic_ncols=True,
        )
        try:
            await asyncio.gather(*[_worker_loop(w, queue, pbar, stats) for w in workers])
        except KeyboardInterrupt:
            log.warning("interrupted; saving progress")
            for w in workers:
                await w.shutdown()
        finally:
            pbar.close()
            await browser.close()

    _print_summary(stats, started, len(remaining))

    if not skip_etl and output_csv.exists():
        _run_etl(output_csv, parquet_dir)


def _run_etl(input_csv: Path, parquet_dir: Path) -> None:
    log.info("running ETL on %s", input_csv.name)
    try:
        from etl.pipeline import run_pipeline
        run_pipeline(input_csv, parquet_dir)
    except Exception as e:
        log.error("ETL pipeline failed: %s", e)


# ──────────────── reporting ────────────────


def _print_banner(
    input_path, output_csv, n_workers, max_reviews,
    all_places, completed, remaining, retry_non_terminal,
) -> None:
    reasons = _tally_reasons(completed)
    log.info("=" * 78)
    log.info("REVIEWS SCRAPING ORCHESTRATOR")
    log.info("  input:        %s", input_path.resolve())
    log.info("  output:       %s", output_csv.resolve())
    log.info("  workers:      %d", n_workers)
    log.info("  max reviews:  %d", max_reviews)
    log.info("  total places: %d", len(all_places))
    log.info("  completed:    %d  %s", len(completed), reasons or "")
    log.info("  retry non-terminal: %s", retry_non_terminal)
    log.info("  remaining:    %d", len(remaining))
    log.info("=" * 78)


def _tally_reasons(completed: dict[str, dict]) -> str:
    counts: dict[str, int] = {}
    for rec in completed.values():
        counts[rec["reason"]] = counts.get(rec["reason"], 0) + 1
    return "{ " + ", ".join(f"{k}: {v}" for k, v in sorted(counts.items())) + " }" if counts else ""


def _print_summary(stats: dict, started: float, total: int) -> None:
    elapsed = time.time() - started
    log.info("=" * 78)
    log.info("SCRAPING SUMMARY")
    log.info("  processed:    %d", total - stats["errors"])
    log.info("  errors:       %d", stats["errors"])
    log.info("  reviews:      %d", stats["reviews"])
    log.info("  elapsed:      %.1fs", elapsed)
    if total:
        log.info("  avg/place:    %.1fs", elapsed / total)
    log.info("=" * 78)


# ──────────────── CLI ────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Parallel Google Maps reviews scraper (async Playwright)."
    )
    parser.add_argument("--input", type=str, default=DEFAULT_INPUT_FILE)
    parser.add_argument("--output-dir", type=str, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--parquet-dir", type=str, default=DEFAULT_PARQUET_DIR)
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument(
        "--max-reviews", type=int, default=MAX_REVIEWS_PER_PLACE,
        help="Hard cap per place. 0 = unlimited (pair with PLACE_TIMEOUT).",
    )
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--skip-etl", action="store_true")
    parser.add_argument(
        "--retry-non-terminal", action="store_true",
        help="Re-queue places marked dom_stable/timeout/error on resume.",
    )
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent
    input_path = project_root / args.input
    output_dir = project_root / args.output_dir
    parquet_dir = project_root / args.parquet_dir

    if not input_path.exists():
        log.error("input file not found: %s", input_path)
        sys.exit(1)

    asyncio.run(
        run_orchestrator(
            input_path=input_path,
            output_dir=output_dir,
            parquet_dir=parquet_dir,
            n_workers=args.workers,
            max_reviews=args.max_reviews,
            debug=args.debug,
            skip_etl=args.skip_etl,
            retry_non_terminal=args.retry_non_terminal,
        )
    )


if __name__ == "__main__":
    main()
