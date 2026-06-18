"""Massive geographic orchestrator: traverse Peruvian districts and scrape places.

Resume contract:
  - A district is DONE iff its CSV file exists. Legacy CSVs (no manifest) are
    accepted for back-compat.
  - CSV writes are atomic (`.partial` → os.replace) so half-written files are
    impossible — no size check needed.
  - A `<district>.json` manifest sits beside each CSV with per-category counts,
    timestamps, and the parameters used. Diagnostic only; resume ignores it.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import sys
from contextlib import suppress

# Allow `python orchestrator_peru.py` to find the in-tree packages.
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import crawler.placesCrawlerV2 as crawler  # noqa: E402
from config.constant import categories  # noqa: E402

GEO_REF_CSV = "config/geo_ref_pe.csv"
OUTPUT_ROOT = "data"
CSV_HEADER = (
    "id,url_place,title,category,address,phoneNumber,completePhoneNumber,"
    "domain,url,coor,stars,source_query\n"
)


def sanitize_filename(name: str) -> str:
    """Strip OS-unfriendly characters and collapse spaces to underscores."""
    keep = (" ", ".", "_", "-")
    cleaned = "".join(c for c in name if c.isalnum() or c in keep).rstrip()
    return cleaned.replace(" ", "_")


def write_atomic(path: str, content: str) -> None:
    """Atomic text write via `.partial` → os.replace, with fsync."""
    tmp = f"{path}.partial"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(content)
        f.flush()
        with suppress(OSError, AttributeError):
            os.fsync(f.fileno())
    os.replace(tmp, path)


def write_manifest(path: str, payload: dict) -> None:
    write_atomic(path, json.dumps(payload, ensure_ascii=False, indent=2, default=str))


def write_empty_csv(path: str) -> None:
    """Header-only CSV signalling 'district processed, no results'."""
    write_atomic(path, CSV_HEADER)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="District-level orchestrator for Peru."
    )
    parser.add_argument("--limit", type=int, default=None,
                        help="Max results per category query (default: no limit).")
    parser.add_argument("--concurrent", type=int, default=3,
                        help="Concurrent requests limit (recommended: 3).")
    parser.add_argument("--lang", type=str, default="es")
    parser.add_argument("--country", type=str, default="pe")
    parser.add_argument("--start-idx", type=int, default=0,
                        help="Start index in the district list.")
    parser.add_argument(
        "--deps", type=json.loads, default=None,
        help='Department filter, JSON array. Ex: --deps \'["Lima", "Cusco"]\'',
    )
    return parser


def _district_paths(row: dict, base: str) -> tuple[str, str, str]:
    dep = sanitize_filename(row["departamento"])
    prov = sanitize_filename(row["provincia"])
    dist = sanitize_filename(row["distrito"])
    out_dir = os.path.join(base, dep, prov)
    return out_dir, os.path.join(out_dir, f"{dist}.csv"), os.path.join(out_dir, f"{dist}.json")


def main() -> None:
    args = _build_parser().parse_args()

    if not os.path.exists(GEO_REF_CSV):
        print(f"Critical: '{GEO_REF_CSV}' not found.")
        sys.exit(1)

    with open(GEO_REF_CSV, encoding="utf-8") as f:
        districts = list(csv.DictReader(f))

    print("=== ORCHESTRATOR STARTED ===")
    print(f"Total districts: {len(districts)}")
    print(f"Categories per district: {len(categories)}")
    print("============================")

    for i, row in enumerate(districts):
        if i < args.start_idx:
            continue
        if args.deps and row["departamento"] not in args.deps:
            continue

        out_dir, csv_path, manifest_path = _district_paths(row, OUTPUT_ROOT)
        if os.path.exists(csv_path):
            print(f"[{i+1}/{len(districts)}] ⏭️ SKIPPING: {csv_path} exists")
            continue

        os.makedirs(out_dir, exist_ok=True)
        _process_district(row, i, len(districts), args, csv_path, manifest_path)


def _process_district(
    row: dict, idx: int, total: int, args: argparse.Namespace,
    csv_path: str, manifest_path: str,
) -> None:
    dep, prov, dist = row["departamento"], row["provincia"], row["distrito"]
    print(f"\n[{idx+1}/{total}] 🚀 PROCESSING: {dist}, {prov}, ({dep})")

    queries = [f"{cat} en el distrito de {dist}, {prov}, {dep}, Perú" for cat in categories]
    started_at = dt.datetime.now()

    results = crawler.search_multiple(
        queries, lang=args.lang, country=args.country,
        limit=args.limit, max_concurrent=args.concurrent,
    )
    finished_at = dt.datetime.now()

    if results:
        per_category: dict[str, int] = {}
        for r in results:
            q = r.get("source_query", "")
            per_category[q] = per_category.get(q, 0) + 1
        crawler.save_to_csv(results, csv_path)
    else:
        per_category = dict.fromkeys(queries, 0)
        print("No results across all categories for this district.")
        write_empty_csv(csv_path)

    write_manifest(manifest_path, {
        "department": dep, "province": prov, "district": dist,
        "started_at": started_at, "finished_at": finished_at,
        "elapsed_s": (finished_at - started_at).total_seconds(),
        "queries_count": len(queries),
        "rows_written": len(results) if results else 0,
        "rows_per_query": per_category,
        "lang": args.lang, "country": args.country,
        "limit_per_query": args.limit, "concurrent": args.concurrent,
    })


if __name__ == "__main__":
    main()
