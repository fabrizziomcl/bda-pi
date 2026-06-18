"""Single- or multi-query CLI for Google Maps place scraping."""

from __future__ import annotations

import argparse
import os
import sys
import time

from crawler.placesCrawlerV2 import PlacesCrawler, save_to_csv


def read_queries_from_file(path: str) -> list[str]:
    """Read non-empty, non-comment lines from `path`."""
    try:
        with open(path, encoding="utf-8") as f:
            return [
                line.strip() for line in f
                if line.strip() and not line.strip().startswith("#")
            ]
    except FileNotFoundError:
        print(f"Error: could not find file {path}")
        return []
    except OSError as e:
        print(f"Error reading {path}: {e}")
        return []


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Scrape Google Maps for local services with concurrent processing.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            '  python mapScraperX.py "restaurants in Miami" --limit 50\n'
            "  python mapScraperX.py --queries-file query_example.txt\n"
            "  python mapScraperX.py --queries-file query_example.txt --concurrent 5\n"
        ),
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("query", nargs="?", type=str, help="The search query.")
    group.add_argument("--queries-file", type=str,
                       help="Path to a text file containing one query per line.")

    parser.add_argument("--lang", type=str, default="en")
    parser.add_argument("--country", type=str, default="us")
    parser.add_argument("--limit", type=int,
                        help="Max results per query (total for single query).")
    parser.add_argument("--output-file", type=str, default="data/output.csv")
    parser.add_argument("--concurrent", type=int, default=3,
                        help="Max concurrent queries (recommended 3-5).")
    return parser


def main() -> None:
    args = _build_parser().parse_args()

    output_dir = os.path.dirname(args.output_file)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    import asyncio

    crawler = PlacesCrawler(
        lang=args.lang, country=args.country, max_concurrent=args.concurrent,
    )

    if args.query:
        print(f"Mode: single query — {args.query!r}")
        results = asyncio.run(crawler.search(args.query, limit=args.limit))
    else:
        queries = read_queries_from_file(args.queries_file)
        if not queries:
            print("No valid queries found.")
            sys.exit(1)
        print(f"Mode: {len(queries)} queries, concurrency={args.concurrent}, "
              f"limit per query={args.limit}")
        started = time.time()
        results = asyncio.run(crawler.search_many(queries, limit=args.limit))
        elapsed = time.time() - started
        print(f"\nCompleted in {elapsed:.2f}s "
              f"({elapsed / max(len(queries), 1):.2f}s/query average)")

    if not results:
        print("No results found.")
        return

    save_to_csv(results, args.output_file)
    print("=" * 50)
    print(f"Total results: {len(results)}")
    print(f"File saved to: {args.output_file}")
    print("=" * 50)


if __name__ == "__main__":
    main()
