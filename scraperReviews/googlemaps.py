"""Google Maps review extractor — Playwright async."""

from __future__ import annotations

import logging
import random
import re
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum

from playwright.async_api import (
    BrowserContext,
    Page,
)
from playwright.async_api import (
    Error as PlaywrightError,
)
from playwright.async_api import (
    TimeoutError as PlaywrightTimeout,
)

GM_WEBPAGE = "https://www.google.com/maps/"

# Selectors. Brittle to Google's CSS-in-JS class changes; treat as the
# integration boundary that needs a manual update on layout drift.
REVIEWS_TAB_XPATH = (
    '//button[@role="tab" and ('
    'contains(., "Opiniones") or contains(., "Reseñas") or '
    'contains(., "Reseas") or contains(., "Reviews") or '
    'contains(@aria-label, "Opiniones") or '
    'contains(@aria-label, "Reseñas") or '
    'contains(@aria-label, "Reviews"))]'
)
SORT_BUTTON_SEL = 'button[aria-label*="Ordenar"], button[aria-label*="Sort"]'
SORT_OPTION_SEL = 'div[role="menuitemradio"]'
REVIEW_BLOCK_SEL = "div.jftiEf.fontBodyMedium"
EXPAND_BUTTON_SEL = "button.w8nwRe.kyuRq"
SCROLL_DIV_SEL = "div.m6QErb.DxyBCb.kA9KIf.dS8AEf"
REVIEWS_FALLBACK_SELECTORS = (
    'button[aria-label*="opiniones"]',
    'button[aria-label*="reseñas"]',
    'button[aria-label*="reseas"]',
    'button[aria-label*="reviews"]',
    "div.F7nice",
)

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

EXTRACT_REVIEWS_JS = """
(offset) => {
    const blocks = document.querySelectorAll('div.jftiEf.fontBodyMedium');
    return Array.from(blocks).slice(offset).map(r => ({
        id_review:     r.getAttribute('data-review-id'),
        username:      r.getAttribute('aria-label'),
        caption:       r.querySelector('span.wiI7pd')?.textContent?.replace(/[\\r\\n\\t]/g, ' ') || null,
        rating_label:  r.querySelector('span.kvMYJc')?.getAttribute('aria-label') || null,
        relative_date: r.querySelector('span.rsqaWe')?.textContent || null,
        n_review_text: r.querySelector('div.RfnDt')?.textContent || null,
        url_user:      r.querySelector('button.WEBjve')?.getAttribute('data-href') || null,
    }));
}
"""


class SortBy(str, Enum):
    """Visible sort options in the Reviews tab dropdown."""

    MOST_RELEVANT = "most_relevant"
    NEWEST = "newest"
    HIGHEST = "highest"
    LOWEST = "lowest"


@dataclass(frozen=True, slots=True)
class _Timeouts:
    """Centralized Playwright timeouts (milliseconds)."""

    page_load_ms: int = 30_000
    title_visible_ms: int = 10_000
    settle_ms: int = 3_000
    click_ms: int = 10_000
    menu_visible_ms: int = 5_000
    tab_load_ms: int = 5_000
    new_reviews_ms: int = 3_000
    expand_click_ms: int = 300
    cookie_dismiss_ms: int = 2_000
    sort_apply_ms: int = 1_000
    fallback_pause_ms: int = 1_000


TIMEOUTS = _Timeouts()


# Visible-label substrings to disambiguate options regardless of Google's
# menu ordering. Lowercased; substring match against text + aria-label.
_SORT_LABELS: dict[SortBy, tuple[str, ...]] = {
    SortBy.MOST_RELEVANT: ("más relevantes", "most relevant"),
    SortBy.NEWEST: ("más recientes", "newest", "most recent"),
    SortBy.HIGHEST: ("calificación más alta", "highest"),
    SortBy.LOWEST: ("calificación más baja", "lowest"),
}

# Approximations: "month" = 30d, "year" = 365d. Google emits relative labels
# only, so `review_date` is coarse; use `relative_date` for exact provenance.
_RELATIVE_UNITS: tuple[tuple[str, Callable[[int], timedelta]], ...] = (
    ("segundo", lambda v: timedelta(seconds=v)),
    ("minuto", lambda v: timedelta(minutes=v)),
    ("hora", lambda v: timedelta(hours=v)),
    ("día", lambda v: timedelta(days=v)),
    ("semana", lambda v: timedelta(weeks=v)),
    ("mes", lambda v: timedelta(days=v * 30)),
    ("año", lambda v: timedelta(days=v * 365)),
    ("second", lambda v: timedelta(seconds=v)),
    ("minute", lambda v: timedelta(minutes=v)),
    ("hour", lambda v: timedelta(hours=v)),
    ("day", lambda v: timedelta(days=v)),
    ("week", lambda v: timedelta(weeks=v)),
    ("month", lambda v: timedelta(days=v * 30)),
    ("year", lambda v: timedelta(days=v * 365)),
)
_EDIT_PREFIXES = ("editado ", "edited ")
_SINGULAR_TOKENS = ("un ", "una ", "a ", "an ")


async def setup_context(context: BrowserContext) -> None:
    """Apply per-context stealth tweaks before any navigation."""
    cores = random.choice((4, 8, 12, 16))
    await context.add_init_script(
        f"""
        Object.defineProperty(navigator, 'webdriver', {{get: () => undefined}});
        Object.defineProperty(navigator, 'hardwareConcurrency', {{get: () => {cores}}});
        Object.defineProperty(navigator, 'devicePixelRatio', {{get: () => 1}});
        Object.defineProperty(navigator, 'maxTouchPoints', {{get: () => 0}});
        Object.defineProperty(navigator, 'platform', {{get: () => 'Win32'}});
        Object.defineProperty(navigator, 'languages', {{get: () => ['es-ES', 'es', 'en']}});
        """
    )


class GoogleMapsScraper:
    """Drive a Playwright `Page` through the Reviews tab of a Maps place."""

    def __init__(self, page: Page, debug: bool = False) -> None:
        self.page = page
        self.debug = debug
        self.logger = logging.getLogger("googlemaps-scraper")

    # ──────────────── public API ────────────────

    async def sort_by(self, url: str, sort: SortBy | str | int = SortBy.NEWEST) -> int:
        """Navigate to `url`, open Reviews tab, apply sort. Returns 0 on success, -1 on failure."""
        await self._load_place_page(url)
        await self._dismiss_cookies()
        if not await self._open_reviews_tab():
            return -1
        if not await self._open_sort_menu():
            return -1
        return await self._select_sort_option(self._resolve_sort(sort))

    async def get_reviews(self, offset: int) -> list[dict]:
        """Scroll, expand truncated bodies, extract reviews past `offset`."""
        await self._scroll()
        await self._wait_for_new_reviews(offset)
        await self._expand_reviews()
        raw = await self.page.evaluate(EXTRACT_REVIEWS_JS, offset)
        retrieval_date = datetime.now()
        return [self._parse_review(r, retrieval_date) for r in raw]

    async def get_account(self, url: str) -> dict:
        """Open `url` and return the place-level metadata block."""
        try:
            await self.page.goto(url.strip(), wait_until="load",
                                 timeout=TIMEOUTS.page_load_ms)
            await self.page.wait_for_timeout(TIMEOUTS.settle_ms)
        except PlaywrightTimeout:
            self.logger.warning("get_account: page load timeout for %s", url)
        await self._dismiss_cookies()
        return await self._parse_place()

    # ──────────────── navigation ────────────────

    async def _load_place_page(self, url: str) -> None:
        try:
            await self.page.goto(url.strip(), wait_until="domcontentloaded",
                                 timeout=TIMEOUTS.page_load_ms)
        except PlaywrightTimeout:
            self.logger.warning("page load timeout: %s", url.strip())
        with suppress(PlaywrightTimeout):
            await self.page.wait_for_selector(
                "h1.DUwDvf", timeout=TIMEOUTS.title_visible_ms
            )
        await self.page.wait_for_timeout(TIMEOUTS.settle_ms)

    async def _dismiss_cookies(self) -> None:
        with suppress(PlaywrightTimeout, PlaywrightError):
            await self.page.locator("text=Rechazar todo").click(
                timeout=TIMEOUTS.cookie_dismiss_ms
            )

    async def _open_reviews_tab(self) -> bool:
        for _ in range(3):
            if await self._click_reviews_tab_xpath():
                return True
            if await self._click_reviews_fallback():
                return True
            await self.page.wait_for_timeout(TIMEOUTS.fallback_pause_ms)
            await self.page.mouse.wheel(0, 300)
        return False

    async def _click_reviews_tab_xpath(self) -> bool:
        try:
            tab = self.page.locator(f"xpath={REVIEWS_TAB_XPATH}")
            if await tab.count() == 0:
                return False
            await tab.first.click(timeout=TIMEOUTS.click_ms // 3)
            await self._wait_for_tab_load()
            return True
        except (PlaywrightTimeout, PlaywrightError):
            return False

    async def _click_reviews_fallback(self) -> bool:
        for sel in REVIEWS_FALLBACK_SELECTORS:
            try:
                loc = self.page.locator(sel).first
                if await loc.count() == 0 or not await loc.is_visible():
                    continue
                await loc.click(timeout=TIMEOUTS.click_ms // 3)
                await self._wait_for_tab_load()
                if await self.page.locator(SORT_BUTTON_SEL).count() > 0:
                    return True
            except (PlaywrightTimeout, PlaywrightError):
                continue
        return False

    async def _wait_for_tab_load(self) -> None:
        with suppress(PlaywrightTimeout):
            await self.page.locator(f"{REVIEW_BLOCK_SEL}, {SORT_BUTTON_SEL}").first.wait_for(
                state="visible", timeout=TIMEOUTS.tab_load_ms
            )

    async def _open_sort_menu(self) -> bool:
        try:
            await self.page.locator(SORT_BUTTON_SEL).first.click(
                timeout=TIMEOUTS.click_ms
            )
            await self.page.locator(SORT_OPTION_SEL).first.wait_for(
                state="visible", timeout=TIMEOUTS.menu_visible_ms
            )
            return True
        except PlaywrightTimeout:
            return False

    # ──────────────── sort selection ────────────────

    @staticmethod
    def _resolve_sort(value: SortBy | str | int) -> SortBy | None:
        """Coerce caller input into a SortBy. Returns None on invalid."""
        if isinstance(value, SortBy):
            return value
        if isinstance(value, int):
            order = (SortBy.MOST_RELEVANT, SortBy.NEWEST, SortBy.HIGHEST, SortBy.LOWEST)
            return order[value] if 0 <= value < len(order) else None
        try:
            return SortBy(value)
        except ValueError:
            return None

    async def _select_sort_option(self, target: SortBy | None) -> int:
        if target is None:
            self.logger.warning("invalid sort value")
            return -1
        options = await self.page.locator(SORT_OPTION_SEL).all()
        if not options:
            return -1

        labels = _SORT_LABELS[target]
        for opt in options:
            if await self._option_matches(opt, labels):
                await opt.click()
                await self.page.wait_for_timeout(TIMEOUTS.sort_apply_ms)
                return 0

        self.logger.warning(
            "sort label miss for %s among %d options — add label to _SORT_LABELS",
            target.value, len(options),
        )
        return -1

    @staticmethod
    async def _option_matches(opt, labels: tuple[str, ...]) -> bool:
        try:
            text = ((await opt.text_content()) or "").lower()
            aria = ((await opt.get_attribute("aria-label")) or "").lower()
        except PlaywrightError:
            return False
        blob = f"{text} {aria}"
        return any(lbl in blob for lbl in labels)

    # ──────────────── review feed ────────────────

    async def _scroll(self) -> None:
        with suppress(PlaywrightError):
            await self.page.locator(SCROLL_DIV_SEL).evaluate(
                "el => el.scrollTop = el.scrollHeight"
            )

    async def _wait_for_new_reviews(self, current_offset: int) -> None:
        with suppress(PlaywrightTimeout):
            await self.page.wait_for_function(
                f'document.querySelectorAll("{REVIEW_BLOCK_SEL}").length > {current_offset}',
                timeout=TIMEOUTS.new_reviews_ms,
            )

    async def _expand_reviews(self) -> None:
        for btn in await self.page.locator(EXPAND_BUTTON_SEL).all():
            try:
                await btn.click(timeout=TIMEOUTS.expand_click_ms)
            except (PlaywrightTimeout, PlaywrightError):
                continue

    # ──────────────── parsing ────────────────

    def _parse_review(self, raw: dict, retrieval_date: datetime) -> dict:
        return {
            "id_review": raw.get("id_review"),
            "caption": raw.get("caption"),
            "relative_date": raw.get("relative_date"),
            "review_date": self._calculate_review_date(
                raw.get("relative_date"), retrieval_date
            ),
            "retrieval_date": retrieval_date,
            "rating": _parse_rating(raw.get("rating_label")),
            "username": raw.get("username"),
            "n_review_user": _parse_first_int(raw.get("n_review_text")),
            "n_photo_user": None,
            "url_user": raw.get("url_user"),
        }

    def _calculate_review_date(
        self, relative: str | None, retrieval: datetime
    ) -> datetime:
        """Convert a relative label ("hace 3 días") into an approximate datetime."""
        if not relative:
            return retrieval
        try:
            s = relative.lower().strip()
            for prefix in _EDIT_PREFIXES:
                if s.startswith(prefix):
                    s = s[len(prefix):]
                    break
            value = _extract_count(s)
            if value is None:
                self.logger.debug("unparseable relative date: %r", relative)
                return retrieval
            for key, delta_fn in _RELATIVE_UNITS:
                if key in s:
                    return retrieval - delta_fn(value)
            self.logger.warning(
                "unrecognized relative-date unit: %r — add to _RELATIVE_UNITS", relative
            )
            return retrieval
        except (ValueError, AttributeError) as e:
            self.logger.warning("failed to parse %r: %s", relative, e)
            return retrieval

    async def _parse_place(self) -> dict:
        return await self.page.evaluate(
            """() => {
                const txt = s => document.querySelector(s)?.textContent?.trim() || null;
                const attr = (s, a) => document.querySelector(s)?.getAttribute(a) || null;
                const infoDivs = document.querySelectorAll('div.Io6YTe.fontBodyMedium');
                return {
                    name: txt('h1.DUwDvf.fontHeadlineLarge'),
                    overall_rating: attr('div.F7nice span.ceNzKf', 'aria-label'),
                    n_reviews_text: txt('div.F7nice'),
                    category: txt('button[jsaction="pane.rating.category"]'),
                    address: infoDivs[0]?.textContent || null,
                };
            }"""
        )


# ──────────────── free helpers ────────────────


def _parse_rating(label: str | None) -> float | None:
    if not label:
        return None
    m = re.search(r"(\d+)", label)
    return float(m.group(1)) if m else None


def _parse_first_int(text: str | None) -> int | None:
    if not text:
        return None
    m = re.search(r"\d+", text)
    if not m:
        return None
    try:
        return int(m.group(0))
    except ValueError:
        return None


def _extract_count(s: str) -> int | None:
    """Pull a leading integer count from a relative-date string, or `1` for singulars."""
    m = re.search(r"(\d+)", s)
    if m:
        return int(m.group(1))
    if any(tok in s for tok in _SINGULAR_TOKENS):
        return 1
    return None
