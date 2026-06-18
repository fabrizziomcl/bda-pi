"""Regression tests for the hardcoded JSON offsets in _extract_place.

Google's tbm=map response is undocumented; we read place data out of fixed
offsets in a ~260-element list. If those offsets shift, the extractor's
_safe_get silently returns defaults instead of raising. These tests build
synthetic results matching the documented offsets and pin the extractor's
output shape.
"""

from __future__ import annotations

from crawler.placesCrawlerV2 import _extract_place


def _build_result(
    place_id: str = "ChIJN1t_tDeuEmsRUsoyG83frY4",
    title: str = "Test Restaurant",
    category: str = "Pizzería",
    address: str = "Av. Test 123, Lima 15001",
    stars: float = 4.5,
    website_url: str = "https://test.com",
    website_domain: str = "test.com",
    lat: float = -12.0464,
    lng: float = -77.0428,
    phone_local: str = "(01) 555-0100",
    phone_intl: str = "+51 1 555-0100",
) -> list:
    r: list = [None] * 260
    r[4] = [None] * 8
    r[4][7] = stars
    r[7] = [website_url, website_domain]
    r[9] = [None, None, lat, lng]
    r[11] = title
    r[13] = [category]
    r[39] = address
    r[78] = place_id
    r[178] = [[None, [[phone_local], [phone_intl]]]]
    return r


def test_extract_place_happy_path_all_fields() -> None:
    place = _extract_place(_build_result(), query="test query")
    assert place is not None
    assert place["id"] == "ChIJN1t_tDeuEmsRUsoyG83frY4"
    assert place["title"] == "Test Restaurant"
    assert place["category"] == "Pizzería"
    assert place["address"] == "Av. Test 123, Lima 15001"
    assert place["stars"] == 4.5
    assert place["domain"] == "test.com"
    assert place["url"] == "https://test.com"
    assert place["coor"] == "-12.0464,-77.0428"
    assert place["phoneNumber"] == "(01) 555-0100"
    assert place["completePhoneNumber"] == "+51 1 555-0100"
    assert place["url_place"] == (
        "https://www.google.com/maps/place/?q=place_id:ChIJN1t_tDeuEmsRUsoyG83frY4"
    )
    assert place["source_query"] == "test query"
    assert "reviews" not in place


def test_extract_place_returns_none_without_place_id() -> None:
    r = _build_result()
    r[78] = None
    assert _extract_place(r, query="x") is None


def test_extract_place_handles_missing_phones() -> None:
    r = _build_result()
    r[178] = None
    place = _extract_place(r, query="x")
    assert place is not None
    assert place["phoneNumber"] == ""
    assert place["completePhoneNumber"] == ""


def test_extract_place_handles_missing_coords() -> None:
    r = _build_result()
    r[9] = None
    place = _extract_place(r, query="x")
    assert place is not None
    assert place["coor"] == ""


def test_extract_place_handles_missing_website() -> None:
    r = _build_result()
    r[7] = None
    place = _extract_place(r, query="x")
    assert place is not None
    assert place["domain"] == ""
    assert place["url"] == ""


def test_extract_place_handles_short_result_list() -> None:
    assert _extract_place([None] * 50, query="x") is None


def test_extract_place_minimal_only_id() -> None:
    r: list = [None] * 260
    r[78] = "ChIJONLYID"
    place = _extract_place(r, query="q")
    assert place is not None
    assert place["id"] == "ChIJONLYID"
    assert place["title"] == ""
    assert place["category"] == ""
    assert place["stars"] == ""
