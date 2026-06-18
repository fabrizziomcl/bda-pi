"""Incremental review monitor — polls mapped places for new reviews."""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import sys
import time
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Protocol, runtime_checkable

import polars as pl
from playwright.async_api import async_playwright
from tqdm import tqdm

from config.scraper_config import (
    DELAY_BETWEEN_PLACES_MAX,
    DELAY_BETWEEN_PLACES_MIN,
    REVIEW_HEADER,
    VIEWPORT,
    WORKER_RETRY_MAX,
)
from googlemaps import (
    GM_WEBPAGE,
    TIMEOUTS,
    UA,
    GoogleMapsScraper,
    SortBy,
    setup_context,
)
from logging_utils import setup_logger

PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_STATE_FILE = PROJECT_ROOT / "data" / "output" / "monitor_state.json"
DEFAULT_OUTPUT_CSV = PROJECT_ROOT / "data" / "output" / "monitor_reviews_new.csv"


log = setup_logger("monitor", log_file="monitor.log")


# ──────────────── sinks (pluggable output) ────────────────


@runtime_checkable
class Sink(Protocol):
    """Output target for new reviews. Implement emit + close."""

    def emit(self, review: dict) -> None: ...
    def close(self) -> None: ...


class CsvSink:
    """Append-only CSV sink. Writes header on first creation."""

    def __init__(self, output_path: Path) -> None:
        self.output_path = output_path
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        new_file = not self.output_path.exists()
        # Long-lived handle (Sink is a stream, not a one-shot writer).
        self._fh = open(self.output_path, "a", encoding="utf-8", newline="\n")  # noqa: SIM115
        self._writer = csv.writer(self._fh, quoting=csv.QUOTE_MINIMAL)
        if new_file:
            self._writer.writerow(REVIEW_HEADER)

    def emit(self, review: dict) -> None:
        self._writer.writerow([review.get(k, "") for k in REVIEW_HEADER])
        self._fh.flush()

    def close(self) -> None:
        with suppress(Exception):
            self._fh.close()


# Future: implement KafkaSink(topic, bootstrap) for streaming integration.


# ──────────────── state ────────────────


@dataclass(slots=True)
class _PlaceWatermark:
    last_review_id: str | None = None
    last_review_date_iso: str | None = None
    last_poll_iso: str | None = None


def load_state(state_path: Path) -> dict:
    if not state_path.exists():
        return {}
    try:
        return json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        log.warning("could not load state (%s): %s — starting fresh", state_path, e)
        return {}


def save_state(state_path: Path, state: dict) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = state_path.with_suffix(".tmp")
    tmp.write_text(
        json.dumps(state, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    tmp.replace(state_path)


# ──────────────── input ────────────────


def load_places(input_path: Path) -> list[dict]:
    df = pl.read_csv(
        str(input_path), infer_schema_length=0, ignore_errors=True,
        encoding="utf8-lossy",
    )
    missing = {"id", "url_place"} - set(df.columns)
    if missing:
        log.error("input CSV missing columns %s; got %s", missing, df.columns)
        sys.exit(1)
    df = df.filter(pl.col("url_place").is_not_null() & (pl.col("url_place") != ""))
    return [
        {"place_id": r["id"], "url": r["url_place"], "title": r.get("title", "")}
        for r in df.iter_rows(named=True)
    ]


# ──────────────── polling ────────────────


async def _poll_place(
    scraper: GoogleMapsScraper,
    place: dict,
    state: dict,
    sink: Sink,
    from_date: datetime | None,
    max_reviews_per_poll: int,
) -> tuple[int, str]:
    """Emit only reviews newer than this place's watermark. Returns (new, status)."""
    place_id = place["place_id"]
    url = place["url"]
    prev = state.get(place_id, {})
    seen_id = prev.get("last_review_id")
    seen_date = _parse_iso(prev.get("last_review_date_iso"))

    if await scraper.sort_by(url, SortBy.NEWEST) != 0:
        return 0, "no-reviews-tab"

    new_count = 0
    offset = 0
    latest_id: str | None = None
    latest_date: datetime | None = None

    while new_count < max_reviews_per_poll:
        batch = await scraper.get_reviews(offset)
        if not batch:
            break
        stop = False
        for r in batch:
            r_id = r.get("id_review")
            r_date = r.get("review_date")
            if latest_id is None:
                latest_id = r_id
                latest_date = r_date if isinstance(r_date, datetime) else None
            if seen_id and r_id == seen_id:
                stop = True
                break
            if from_date and isinstance(r_date, datetime) and r_date < from_date:
                stop = True
                break
            if seen_date and isinstance(r_date, datetime) and r_date <= seen_date:
                stop = True
                break
            sink.emit(_review_record(place_id, url, r))
            new_count += 1
        if stop:
            break
        offset += len(batch)

    if latest_id:
        state[place_id] = {
            "last_review_id": latest_id,
            "last_review_date_iso": latest_date.isoformat() if latest_date else None,
            "last_poll_iso": datetime.now().isoformat(),
        }
    return new_count, "ok"


def _review_record(place_id: str, url: str, r: dict) -> dict:
    return {
        "place_id": place_id,
        "id_review": r.get("id_review"),
        "caption": r.get("caption"),
        "relative_date": r.get("relative_date"),
        "review_date": str(r.get("review_date", "")),
        "retrieval_date": str(r.get("retrieval_date", "")),
        "rating": r.get("rating"),
        "username": r.get("username"),
        "n_review_user": r.get("n_review_user"),
        "n_photo_user": r.get("n_photo_user"),
        "url_user": r.get("url_user"),
        "url_source": url,
    }


def _parse_iso(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


async def run_once(
    places: list[dict],
    state: dict,
    sink: Sink,
    from_date: datetime | None,
    max_reviews_per_poll: int,
    headless: bool,
) -> dict:
    """Single sweep over `places`. Updates `state` in-place; caller persists it."""
    stats = {"polled": 0, "new_reviews": 0, "errors": 0, "no_reviews_tab": 0}
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu",
            ],
        )
        context = await browser.new_context(
            user_agent=UA, locale="es-ES", viewport=VIEWPORT,
        )
        await setup_context(context)
        page = await context.new_page()
        page.set_default_timeout(TIMEOUTS.title_visible_ms)
        await page.goto(GM_WEBPAGE, wait_until="load")
        scraper = GoogleMapsScraper(page)

        for place in tqdm(places, desc="Polling", unit="place"):
            for attempt in range(WORKER_RETRY_MAX):
                try:
                    new_count, status = await _poll_place(
                        scraper, place, state, sink,
                        from_date, max_reviews_per_poll,
                    )
                    stats["polled"] += 1
                    stats["new_reviews"] += new_count
                    if status == "no-reviews-tab":
                        stats["no_reviews_tab"] += 1
                    break
                except Exception as e:
                    log.warning(
                        "[%s] attempt %d failed: %s",
                        place["place_id"], attempt + 1, e,
                    )
                    if attempt == WORKER_RETRY_MAX - 1:
                        stats["errors"] += 1
            await asyncio.sleep(
                (DELAY_BETWEEN_PLACES_MIN + DELAY_BETWEEN_PLACES_MAX) / 2
            )

        await context.close()
        await browser.close()
    return stats


async def run_loop(
    input_path: Path,
    state_path: Path,
    sink: Sink,
    from_date: datetime | None,
    interval_s: int,
    max_reviews_per_poll: int,
    headless: bool,
) -> None:
    """Sweep forever every `interval_s` seconds. interval_s<=0 → single sweep."""
    while True:
        places = load_places(input_path)
        state = load_state(state_path)
        log.info("sweep start: %d places, %d watermarks", len(places), len(state))
        t0 = time.time()
        stats = await run_once(
            places, state, sink, from_date, max_reviews_per_poll, headless,
        )
        save_state(state_path, state)
        dt = time.time() - t0
        log.info(
            "sweep done in %.1fs: polled=%d new=%d errors=%d no_reviews_tab=%d",
            dt, stats["polled"], stats["new_reviews"],
            stats["errors"], stats["no_reviews_tab"],
        )
        if interval_s <= 0:
            break
        await asyncio.sleep(max(0, interval_s - dt))


# ──────────────── CLI ────────────────


def _parse_date(s: str) -> datetime:
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"invalid date {s!r}: use YYYY-MM-DD") from e


def main() -> None:
    parser = argparse.ArgumentParser(description="Incremental review monitor (async).")
    parser.add_argument("--input", type=Path, required=True,
                        help="Places CSV with `id` and `url_place` columns.")
    parser.add_argument("--from-date", type=_parse_date, default=None,
                        help="Cutoff date YYYY-MM-DD; older reviews are ignored.")
    parser.add_argument("--state-file", type=Path, default=DEFAULT_STATE_FILE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_CSV)
    parser.add_argument("--interval", type=int, default=0,
                        help="Seconds between sweeps. 0 = run once and exit.")
    parser.add_argument("--once", action="store_true",
                        help="Force single sweep (overrides --interval).")
    parser.add_argument("--max-reviews-per-poll", type=int, default=200)
    parser.add_argument("--debug", action="store_true",
                        help="Run with a visible browser window.")
    args = parser.parse_args()

    if not args.input.exists():
        log.error("input not found: %s", args.input)
        sys.exit(1)

    interval = 0 if args.once else args.interval
    sink: Sink = CsvSink(args.output)
    try:
        asyncio.run(run_loop(
            input_path=args.input,
            state_path=args.state_file,
            sink=sink,
            from_date=args.from_date,
            interval_s=interval,
            max_reviews_per_poll=args.max_reviews_per_poll,
            headless=not args.debug,
        ))
    except KeyboardInterrupt:
        log.info("interrupted by user")
    finally:
        sink.close()


if __name__ == "__main__":
    main()
