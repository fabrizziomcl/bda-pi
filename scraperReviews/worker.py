"""Async review-scraping worker driven by a shared Chromium browser."""

from __future__ import annotations

import asyncio
import csv
import logging
import os
import random
import time
from collections.abc import Iterable
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from playwright.async_api import Browser

from config.scraper_config import (
    DELAY_BETWEEN_PLACES_MAX,
    DELAY_BETWEEN_PLACES_MIN,
    MAX_REVIEWS_PER_PLACE,
    PLACE_TIMEOUT,
    VIEWPORT,
    WORKER_RETRY_BACKOFF_BASE,
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

log = logging.getLogger("reviews-worker")


# DOM-stable detection: probe the feed up to N times with exponential backoff
# before declaring end-of-feed. Total wall time at MAX=5 is ~31s, generous
# enough to survive transient rate-limiting without false termination.
STABILITY_MAX_PROBES = 5
STABILITY_BACKOFF_BASE_S = 1.0

# Reasons recorded in completed_places.txt. Terminal reasons mean "no retry".
TERMINAL_REASONS = frozenset({"ok", "no_reviews_tab"})


@dataclass(slots=True)
class WorkerContext:
    """Shared resources injected into every `ReviewWorker`."""

    browser: Browser
    output_path: Path
    completed_path: Path
    csv_lock: asyncio.Lock
    completed_lock: asyncio.Lock
    max_reviews: int | None = MAX_REVIEWS_PER_PLACE
    debug: bool = False

    def __post_init__(self) -> None:
        if self.max_reviews is not None and self.max_reviews <= 0:
            self.max_reviews = None


@dataclass(slots=True)
class _FeedOutcome:
    scraped: int
    reason: str


class ReviewWorker:
    """Consume places from a queue; emit reviews to the shared output."""

    def __init__(self, worker_id: int, ctx: WorkerContext) -> None:
        self.worker_id = worker_id
        self.ctx = ctx
        self._scraper: GoogleMapsScraper | None = None
        self._page = None
        self._context = None

    # ──────────────── public API ────────────────

    async def scrape_place(
        self, place_id: str, url: str, sort: SortBy = SortBy.NEWEST
    ) -> int:
        """Scrape one place. Returns reviews written, or -1 on hard failure."""
        await self._ensure_scraper()
        assert self._scraper is not None

        for attempt in range(WORKER_RETRY_MAX):
            try:
                if await self._scraper.sort_by(url, sort) != 0:
                    log.info("[W%d] no reviews tab for %s", self.worker_id, place_id)
                    await self._mark_completed(place_id, 0, "no_reviews_tab")
                    return 0

                outcome = await self._scrape_feed(place_id, url)
                await self._mark_completed(place_id, outcome.scraped, outcome.reason)
                return outcome.scraped

            except Exception as e:
                log.error(
                    "[W%d] error on %s (attempt %d/%d): %s",
                    self.worker_id, place_id, attempt + 1, WORKER_RETRY_MAX, e,
                )
                if attempt < WORKER_RETRY_MAX - 1:
                    await asyncio.sleep(WORKER_RETRY_BACKOFF_BASE * (2 ** attempt))
                    if not await self._reinit_scraper_safely():
                        await self._mark_completed(place_id, 0, "error")
                        return -1

        await self._mark_completed(place_id, 0, "error")
        return -1

    async def add_delay(self) -> None:
        """Random delay between consecutive places — anti-bot smoothing."""
        await asyncio.sleep(
            random.uniform(DELAY_BETWEEN_PLACES_MIN, DELAY_BETWEEN_PLACES_MAX)
        )

    async def shutdown(self) -> None:
        await self._close_scraper()
        log.info("[W%d] shutdown complete", self.worker_id)

    # ──────────────── scraping loop ────────────────

    async def _scrape_feed(self, place_id: str, url: str) -> _FeedOutcome:
        assert self._scraper is not None
        scraped = 0
        offset = 0
        stable_probes = 0
        start = time.time()
        cap = self.ctx.max_reviews

        while cap is None or offset < cap:
            if time.time() - start > PLACE_TIMEOUT:
                log.warning(
                    "[W%d] timeout on %s after %d reviews",
                    self.worker_id, place_id, scraped,
                )
                return _FeedOutcome(scraped, "timeout")

            batch = await self._scraper.get_reviews(offset)
            if not batch:
                if stable_probes >= STABILITY_MAX_PROBES:
                    log.info(
                        "[W%d] DOM stable on %s after %d probes (%d reviews)",
                        self.worker_id, place_id, stable_probes, scraped,
                    )
                    return _FeedOutcome(scraped, "dom_stable")
                await asyncio.sleep(STABILITY_BACKOFF_BASE_S * (2 ** stable_probes))
                stable_probes += 1
                continue

            stable_probes = 0
            await self._append_reviews(_rows_from_batch(place_id, url, batch))
            scraped += len(batch)
            offset += len(batch)

        return _FeedOutcome(scraped, "ok")

    # ──────────────── durable writes ────────────────

    async def _append_reviews(self, rows: Iterable[list]) -> None:
        async with self.ctx.csv_lock:
            _durable_append(self.ctx.output_path, rows)

    async def _mark_completed(self, place_id: str, scraped: int, reason: str) -> None:
        line = (
            f"{place_id},{scraped},{reason},"
            f"{datetime.now().isoformat(timespec='seconds')}\n"
        )
        async with self.ctx.completed_lock:
            _durable_append(self.ctx.completed_path, line)

    # ──────────────── scraper lifecycle ────────────────

    async def _ensure_scraper(self) -> None:
        if self._scraper is None:
            await self._open_scraper()

    async def _open_scraper(self) -> None:
        self._context = await self.ctx.browser.new_context(
            user_agent=UA, locale="es-ES", viewport=VIEWPORT,
        )
        await setup_context(self._context)
        self._page = await self._context.new_page()
        self._page.set_default_timeout(TIMEOUTS.title_visible_ms)
        await self._page.goto(GM_WEBPAGE, wait_until="load")
        self._scraper = GoogleMapsScraper(self._page, debug=self.ctx.debug)

    async def _close_scraper(self) -> None:
        self._scraper = None
        for attr in ("_page", "_context"):
            obj = getattr(self, attr)
            if obj is not None:
                with suppress(Exception):
                    await obj.close()
                setattr(self, attr, None)

    async def _reinit_scraper_safely(self) -> bool:
        await self._close_scraper()
        try:
            await self._open_scraper()
            return True
        except Exception as e:
            log.error("[W%d] failed to reinitialize: %s", self.worker_id, e)
            return False


# ──────────────── module-level helpers ────────────────


def _rows_from_batch(place_id: str, url: str, batch: list[dict]) -> list[list]:
    return [
        [
            place_id, r.get("id_review"), r.get("caption"),
            r.get("relative_date"), str(r.get("review_date", "")),
            str(r.get("retrieval_date", "")), r.get("rating"),
            r.get("username"), r.get("n_review_user"),
            r.get("n_photo_user"), r.get("url_user"), url,
        ]
        for r in batch
    ]


def _durable_append(path: Path, payload: str | Iterable[Iterable]) -> None:
    """Append + flush + fsync. Tolerates non-POSIX targets best-effort."""
    with open(path, "a", encoding="utf-8", newline="\n") as f:
        if isinstance(payload, str):
            f.write(payload)
        else:
            csv.writer(f, quoting=csv.QUOTE_MINIMAL).writerows(payload)
        f.flush()
        with suppress(OSError, AttributeError):
            os.fsync(f.fileno())
