"""Tests for GoogleMapsScraper._calculate_review_date.

Cover Spanish + English variants, the edited prefix, and the
unknown-unit case that the previous implementation swallowed silently.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pytest

from googlemaps import GoogleMapsScraper


@pytest.fixture
def scraper() -> GoogleMapsScraper:
    """A bare instance — _calculate_review_date doesn't touch self.page."""
    return GoogleMapsScraper(page=None, debug=False)  # type: ignore[arg-type]


RETRIEVAL = datetime(2026, 6, 15, 12, 0, 0)


SPANISH_CASES = [
    ("hace 3 segundos", timedelta(seconds=3)),
    ("hace 5 minutos", timedelta(minutes=5)),
    ("hace 2 horas", timedelta(hours=2)),
    ("hace 4 días", timedelta(days=4)),
    ("hace 1 semana", timedelta(weeks=1)),
    ("hace 6 meses", timedelta(days=6 * 30)),
    ("hace 2 años", timedelta(days=2 * 365)),
]

ENGLISH_CASES = [
    ("3 seconds ago", timedelta(seconds=3)),
    ("5 minutes ago", timedelta(minutes=5)),
    ("2 hours ago", timedelta(hours=2)),
    ("4 days ago", timedelta(days=4)),
    ("1 week ago", timedelta(weeks=1)),
    ("6 months ago", timedelta(days=6 * 30)),
    ("2 years ago", timedelta(days=2 * 365)),
]


@pytest.mark.parametrize("label,expected_delta", SPANISH_CASES,
                         ids=[c[0] for c in SPANISH_CASES])
def test_spanish_numeric_units(scraper, label, expected_delta) -> None:
    assert scraper._calculate_review_date(label, RETRIEVAL) == RETRIEVAL - expected_delta


def test_spanish_singular(scraper) -> None:
    assert scraper._calculate_review_date("hace un mes", RETRIEVAL) == \
        RETRIEVAL - timedelta(days=30)


def test_spanish_edited_prefix(scraper) -> None:
    assert scraper._calculate_review_date("Editado hace 2 días", RETRIEVAL) == \
        RETRIEVAL - timedelta(days=2)


@pytest.mark.parametrize("label,expected_delta", ENGLISH_CASES,
                         ids=[c[0] for c in ENGLISH_CASES])
def test_english_numeric_units(scraper, label, expected_delta) -> None:
    assert scraper._calculate_review_date(label, RETRIEVAL) == RETRIEVAL - expected_delta


def test_english_singular(scraper) -> None:
    assert scraper._calculate_review_date("a year ago", RETRIEVAL) == \
        RETRIEVAL - timedelta(days=365)


def test_english_edited_prefix(scraper) -> None:
    assert scraper._calculate_review_date("Edited 2 days ago", RETRIEVAL) == \
        RETRIEVAL - timedelta(days=2)


def test_unknown_unit_logs_warning(scraper, caplog) -> None:
    """Unknown unit must warn; silent failure hides locale drift."""
    with caplog.at_level(logging.WARNING, logger="googlemaps-scraper"):
        result = scraper._calculate_review_date("3 fortnights ago", RETRIEVAL)
    assert result == RETRIEVAL
    assert any(
        "unrecognized relative-date unit" in rec.message.lower()
        for rec in caplog.records
    )


def test_empty_input(scraper) -> None:
    assert scraper._calculate_review_date("", RETRIEVAL) == RETRIEVAL
    assert scraper._calculate_review_date(None, RETRIEVAL) == RETRIEVAL


def test_no_digit_no_singular(scraper) -> None:
    assert scraper._calculate_review_date("recently", RETRIEVAL) == RETRIEVAL
