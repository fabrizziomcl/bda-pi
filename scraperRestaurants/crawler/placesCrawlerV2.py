"""Async crawler for Google Maps tbm=map search results."""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import os
import random
import re
from collections.abc import Iterable
from contextlib import suppress
from dataclasses import dataclass
from typing import Any, Final, TypeVar
from urllib.parse import quote

import aiohttp
from tqdm import tqdm

logger = logging.getLogger(__name__)

T = TypeVar("T")

HEADERS: Final[dict[str, str]] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

XSSI_PREFIX: Final[str] = ")]}'"
RESULTS_INDEX: Final[int] = 64       # data[64] holds the result list
HTTP_TIMEOUT_S: Final[int] = 20

CSV_COLUMNS: Final[tuple[str, ...]] = (
    "id", "url_place", "title", "category", "address",
    "phoneNumber", "completePhoneNumber", "domain", "url",
    "coor", "stars", "source_query",
)


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Retry behaviour for transient HTTP failures."""

    retry_statuses: frozenset[int] = frozenset({429, 500, 502, 503, 504})
    max_attempts: int = 4
    backoff_base_s: float = 1.5
    backoff_cap_s: float = 30.0
    jitter_factor: float = 0.2

    def backoff(self, attempt: int) -> float:
        sleep_s = min(self.backoff_base_s * (2 ** attempt), self.backoff_cap_s)
        jitter = 1.0 + random.uniform(-self.jitter_factor, self.jitter_factor)
        return sleep_s * jitter


def _safe_get(obj: Any, *indices: int | str, default: T | None = None) -> T | None:
    """Navigate nested containers; return `default` on failure or terminal None."""
    try:
        for idx in indices:
            obj = obj[idx]
    except (IndexError, TypeError, KeyError):
        return default
    return default if obj is None else obj


def _extract_place(result: list, query: str) -> dict | None:
    """Map a single Google result entry (data[64][i][1]) to our schema.

    Hardcoded offsets reflect the tbm=map payload shape as of 2026; see
    tests/test_extractor_shape.py for the contract and an update playbook
    when Google rearranges the fields.
    """
    place_id = _safe_get(result, 78)
    if not place_id:
        return None

    place = {
        "id": place_id,
        "url_place": f"https://www.google.com/maps/place/?q=place_id:{place_id}",
        "title": _safe_get(result, 11, default=""),
        "category": _safe_get(result, 13, 0, default=""),
        "address": _safe_get(result, 39, default=""),
        "phoneNumber": _safe_get(result, 178, 0, 1, 0, 0, default=""),
        "completePhoneNumber": _safe_get(result, 178, 0, 1, 1, 0, default=""),
        "domain": _safe_get(result, 7, 1, default=""),
        "url": _safe_get(result, 7, 0, default=""),
        "coor": "",
        "stars": _safe_get(result, 4, 7, default=""),
        "source_query": query,
    }

    lat = _safe_get(result, 9, 2)
    lng = _safe_get(result, 9, 3)
    if lat is not None and lng is not None:
        place["coor"] = f"{lat},{lng}"
    return place


class PlacesCrawler:
    """High-level facade around the two-step Maps + tbm=map endpoint pair."""

    def __init__(
        self,
        *,
        lang: str = "en",
        country: str = "us",
        max_concurrent: int = 3,
        retry: RetryPolicy | None = None,
    ) -> None:
        self.lang = lang
        self.country = country
        self._sem = asyncio.Semaphore(max_concurrent)
        self._retry = retry or RetryPolicy()
        self._connector_limit = max_concurrent * 2

    # ──────────────── public API ────────────────

    async def search(
        self, query: str, *, limit: int | None = None
    ) -> list[dict]:
        """Run one query and return collected places."""
        async with self._open_session() as session:
            return await self._search_one(session, query, limit=limit)

    async def search_many(
        self, queries: list[str], *, limit: int | None = None
    ) -> list[dict]:
        """Run queries concurrently over a shared session."""
        async with self._open_session() as session:
            tasks = [self._search_one(session, q, limit=limit) for q in queries]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        out: list[dict] = []
        for query, res in zip(queries, results):
            if isinstance(res, Exception):
                logger.error("query %r raised: %s", query, res)
            else:
                out.extend(res)
        return out

    # ──────────────── internals ────────────────

    def _open_session(self) -> aiohttp.ClientSession:
        return aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=True, limit=self._connector_limit)
        )

    async def _search_one(
        self,
        session: aiohttp.ClientSession,
        query: str,
        *,
        limit: int | None,
    ) -> list[dict]:
        async with self._sem:
            return await self._collect_places(session, query, limit)

    async def _collect_places(
        self,
        session: aiohttp.ClientSession,
        query: str,
        limit: int | None,
    ) -> list[dict]:
        pbar = tqdm(desc=f"Scraping '{query[:30]}'", unit="results", leave=False)
        result: list[dict] = []
        try:
            search_url = await self._resolve_search_url(session, query)
            if not search_url:
                pbar.set_postfix({"status": "no-url"})
                return result

            seen: set[str] = set()
            start = 0
            while True:
                places, raw_count = await self._fetch_page(session, search_url, query, start)
                if raw_count == 0:
                    break

                new_places = [p for p in places if p["id"] not in seen]
                if not new_places:
                    break

                for place in new_places:
                    seen.add(place["id"])
                    result.append(place)
                    pbar.update(1)
                    pbar.set_postfix({"Total": len(result)})
                    if limit and len(result) >= limit:
                        return result
                start += raw_count
        except Exception as e:
            logger.error("[%s] unhandled exception: %s", query, e)
            pbar.set_postfix({"Error": str(e)[:30]})
        finally:
            pbar.close()
        logger.info("[%s] done — %d result(s)", query, len(result))
        return result

    async def _resolve_search_url(
        self, session: aiohttp.ClientSession, query: str
    ) -> str | None:
        """Fetch /maps/search and extract the canonical tbm=map URL."""
        maps_url = (
            f"https://www.google.com/maps/search/{quote(query)}"
            f"?hl={self.lang}&gl={self.country}"
        )
        html = await self._fetch_with_retry(session, maps_url, query, "maps_page")
        if html is None:
            return None

        match = re.search(r'href="(/search\?tbm=map[^"]+)"', html)
        if not match:
            logger.error(
                "[%s] no pb= URL in maps page — consent wall or bot block? snippet: %r",
                query, html[:300],
            )
            return None
        return "https://www.google.com" + match.group(1).replace("&amp;", "&")

    async def _fetch_page(
        self,
        session: aiohttp.ClientSession,
        search_url: str,
        query: str,
        start: int,
    ) -> tuple[list[dict], int]:
        """Return (places, raw_count). raw_count drives the pagination cursor."""
        url = search_url if start == 0 else f"{search_url}&start={start}"
        body = await self._fetch_with_retry(session, url, query, f"tbm_map_start={start}")
        if body is None:
            return [], 0

        if not body.startswith(XSSI_PREFIX):
            logger.error("[%s] missing XSSI prefix at start=%d: %r",
                         query, start, body[:80])
            return [], 0
        body = body[len(XSSI_PREFIX):].strip()

        try:
            data = json.loads(body)
        except json.JSONDecodeError as e:
            logger.error("[%s] JSON parse error at start=%d: %s", query, start, e)
            return [], 0

        results_array = _safe_get(data, RESULTS_INDEX)
        if results_array is None:
            return [], 0

        places: list[dict] = []
        for entry in results_array:
            if not isinstance(entry, list) or len(entry) < 2:
                continue
            inner = entry[1]
            if not isinstance(inner, list):
                continue
            try:
                place = _extract_place(inner, query)
                if place:
                    places.append(place)
            except Exception as e:
                logger.warning("[%s] extractor error: %s", query, e)
        return places, len(results_array)

    async def _fetch_with_retry(
        self,
        session: aiohttp.ClientSession,
        url: str,
        query: str,
        label: str,
    ) -> str | None:
        """GET with retry on 429/5xx; honour Retry-After when given."""
        last_status: int | None = None
        for attempt in range(self._retry.max_attempts):
            try:
                async with session.get(
                    url, headers=HEADERS,
                    timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT_S),
                ) as resp:
                    last_status = resp.status
                    if resp.status == 200:
                        return await resp.text()
                    if resp.status not in self._retry.retry_statuses:
                        logger.error("[%s] %s HTTP %d (terminal)",
                                     query, label, resp.status)
                        return None
                    sleep_s = self._compute_backoff(resp.headers.get("Retry-After"), attempt)
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt == self._retry.max_attempts - 1:
                    logger.error("[%s] %s exhausted retries: %s", query, label, e)
                    return None
                sleep_s = self._retry.backoff(attempt)
                logger.warning("[%s] %s transport error: %s; retry in %.1fs",
                               query, label, e, sleep_s)
                await asyncio.sleep(sleep_s)
                continue
            except Exception as e:
                logger.error("[%s] %s unexpected: %s", query, label, e)
                return None

            if attempt < self._retry.max_attempts - 1:
                logger.warning("[%s] %s HTTP %d; retry in %.1fs",
                               query, label, last_status, sleep_s)
                await asyncio.sleep(sleep_s)
        logger.error("[%s] %s gave up after %d attempts (last %s)",
                     query, label, self._retry.max_attempts, last_status)
        return None

    def _compute_backoff(self, retry_after: str | None, attempt: int) -> float:
        if retry_after and retry_after.isdigit():
            return min(int(retry_after), self._retry.backoff_cap_s)
        return self._retry.backoff(attempt)


# ──────────────── persistence ────────────────


def save_to_csv(data: Iterable[dict], filename: str = "data/output.csv") -> None:
    """Write places to CSV atomically (`.partial` → os.replace + fsync)."""
    rows = list(data)
    if not rows:
        logger.info("save_to_csv: no data")
        return

    for record in rows:
        for col in CSV_COLUMNS:
            record.setdefault(col, "")

    tmp = f"{filename}.partial"
    try:
        with open(tmp, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
            f.flush()
            with suppress(OSError, AttributeError):
                os.fsync(f.fileno())
        os.replace(tmp, filename)
        logger.info("saved %d rows to %s", len(rows), filename)
    except Exception as e:
        logger.error("save_to_csv failed for %s: %s", filename, e)


# ──────────────── back-compat free functions ────────────────


def search(query: str, lang: str, country: str, limit: int | None) -> list[dict]:
    """Sync wrapper for single-query search."""
    crawler = PlacesCrawler(lang=lang, country=country, max_concurrent=1)
    return asyncio.run(crawler.search(query, limit=limit))


def search_multiple(
    queries: list[str],
    lang: str,
    country: str,
    limit: int | None,
    max_concurrent: int = 3,
) -> list[dict]:
    """Sync wrapper for many-query search."""
    crawler = PlacesCrawler(lang=lang, country=country, max_concurrent=max_concurrent)
    return asyncio.run(crawler.search_many(queries, limit=limit))


async def search_async(
    query: str,
    lang: str,
    country: str,
    limit: int | None,
    semaphore: asyncio.Semaphore | None = None,
    session: aiohttp.ClientSession | None = None,
) -> list[dict]:
    """Async wrapper preserved for legacy callers. Prefer PlacesCrawler.search()."""
    crawler = PlacesCrawler(lang=lang, country=country, max_concurrent=1)
    if semaphore is not None:
        crawler._sem = semaphore  # noqa: SLF001 — legacy bridge
    if session is None:
        return await crawler.search(query, limit=limit)
    return await crawler._search_one(session, query, limit=limit)  # noqa: SLF001


async def search_multiple_async(
    queries: list[str],
    lang: str,
    country: str,
    limit: int | None,
    max_concurrent: int = 3,
) -> list[dict]:
    """Async wrapper preserved for legacy callers."""
    crawler = PlacesCrawler(lang=lang, country=country, max_concurrent=max_concurrent)
    return await crawler.search_many(queries, limit=limit)
