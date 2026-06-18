"""Recall-aware worker benchmark.

Picking workers by throughput alone hides silent data loss: a config that
finishes fast because it dropped reviews looks great in a time-only table.
This benchmark measures completeness against a golden dataset and treats a
config as eligible only if it clears a recall threshold.

Workflow:
    python utils/benchmark_workers.py generate-golden \\
        --sample-csv data/test/sample_places.csv \\
        --golden-out data/test/golden_reviews.json
    python utils/benchmark_workers.py bench \\
        --sample-csv data/test/sample_places.csv \\
        --golden data/test/golden_reviews.json \\
        --configs 1,4,8,12 \\
        --min-recall 0.99
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import sys
import time
from dataclasses import dataclass
from pathlib import Path

# Adjust sys.path before in-tree imports so this script runs standalone.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(PROJECT_ROOT)

import polars as pl  # noqa: E402  (depends on sys.path adjustment above)
from playwright.async_api import async_playwright  # noqa: E402
from tqdm import tqdm  # noqa: E402

from config.scraper_config import COMPLETED_PLACES_FILENAME, RAW_OUTPUT_FILENAME  # noqa: E402
from orchestrator import init_output_csv, load_places  # noqa: E402
from worker import ReviewWorker, WorkerContext  # noqa: E402

# ──────────────── ground-truth I/O ────────────────


def load_golden(path: Path) -> dict[str, int]:
    """Read {place_id: expected_review_count}."""
    with open(path, encoding="utf-8") as f:
        return {k: int(v) for k, v in json.load(f).items()}


def save_golden(path: Path, counts: dict[str, int]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(counts, f, ensure_ascii=False, indent=2, sort_keys=True)


def count_reviews_by_place(csv_path: Path) -> dict[str, int]:
    """Distinct (place_id, id_review) count per place from a scraped CSV."""
    df = pl.read_csv(
        str(csv_path), infer_schema_length=0, ignore_errors=True,
        encoding="utf8-lossy",
    )
    if df.is_empty() or "place_id" not in df.columns:
        return {}
    if "id_review" in df.columns:
        df = df.unique(subset=["place_id", "id_review"])
    return {
        row["place_id"]: row["count"]
        for row in df.group_by("place_id").agg(pl.len().alias("count")).iter_rows(named=True)
    }


# ──────────────── benchmark passes ────────────────


@dataclass(slots=True)
class _RunStats:
    workers: int
    elapsed_s: float
    reviews_emitted: int
    errors: int


async def _run_workers(
    places: list[dict], n_workers: int, max_reviews: int,
    output_csv: Path, completed_path: Path,
) -> _RunStats:
    """Run N workers over `places` once. Returns raw timing + counts."""
    init_output_csv(output_csv)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
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
        )

        queue: asyncio.Queue = asyncio.Queue()
        for p in places:
            queue.put_nowait(p)

        workers = [ReviewWorker(i, ctx) for i in range(n_workers)]
        stats = {"reviews": 0, "errors": 0}
        start = time.time()
        pbar = tqdm(
            total=len(places), desc=f"  {n_workers}w", unit="pl",
            leave=False, dynamic_ncols=True,
        )

        async def loop(worker: ReviewWorker) -> None:
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
                pbar.update(1)
                await worker.add_delay()
            await worker.shutdown()

        await asyncio.gather(*[loop(w) for w in workers])
        pbar.close()
        await browser.close()

    return _RunStats(
        workers=n_workers,
        elapsed_s=time.time() - start,
        reviews_emitted=stats["reviews"],
        errors=stats["errors"],
    )


def _compute_recall(
    scraped: dict[str, int], golden: dict[str, int]
) -> dict:
    """Aggregate + per-place recall metrics."""
    per_place: list[float] = []
    perfect = 0
    over_expected = 0
    for pid, expected in golden.items():
        got = scraped.get(pid, 0)
        if expected <= 0:
            recall = 1.0
        else:
            recall = min(got / expected, 1.0)
            if got > expected:
                over_expected += 1
        per_place.append(recall)
        if recall >= 0.9999:
            perfect += 1
    return {
        "recall_per_place": per_place,
        "mean_recall": sum(per_place) / max(len(per_place), 1),
        "min_recall": min(per_place) if per_place else 0.0,
        "places_perfect": perfect,
        "places_over_expected": over_expected,
    }


# ──────────────── sub-commands ────────────────


async def cmd_generate_golden(args: argparse.Namespace) -> int:
    """Run a single-worker, no-cap pass to seed ground truth."""
    sample = PROJECT_ROOT / args.sample_csv
    out = PROJECT_ROOT / args.golden_out
    if not sample.exists():
        print(f"Error: sample CSV not found: {sample}")
        return 1

    places = load_places(sample)
    print(f"[golden] {len(places)} places — 1 worker, max-reviews={args.max_reviews}")

    work_dir = PROJECT_ROOT / "data" / "benchmark" / "golden"
    if work_dir.exists():
        shutil.rmtree(work_dir)
    work_dir.mkdir(parents=True)
    output_csv = work_dir / RAW_OUTPUT_FILENAME
    completed_path = work_dir / COMPLETED_PLACES_FILENAME

    stats = await _run_workers(places, 1, args.max_reviews, output_csv, completed_path)
    counts = count_reviews_by_place(output_csv)
    for p in places:
        counts.setdefault(p["place_id"], 0)
    save_golden(out, counts)

    print(f"[golden] done in {stats.elapsed_s:.1f}s")
    print(f"[golden] places: {len(counts)} | reviews: {sum(counts.values()):,}")
    print(f"[golden] saved: {out}")
    return 0


async def cmd_bench(args: argparse.Namespace) -> int:
    """Compare worker configs by completeness and time."""
    sample = PROJECT_ROOT / args.sample_csv
    golden_path = PROJECT_ROOT / args.golden
    if not sample.exists() or not golden_path.exists():
        print("Error: need both --sample-csv and --golden to exist.")
        return 1

    golden = load_golden(golden_path)
    places = [p for p in load_places(sample) if p["place_id"] in golden]
    if not places:
        print("Error: no overlap between sample and golden.")
        return 1

    worker_counts = [int(x.strip()) for x in args.configs.split(",")]
    print(f"Benchmark: {len(places)} places, max {args.max_reviews} reviews/place")
    print(f"Configs: {worker_counts} workers")
    print(f"Min-recall threshold: {args.min_recall:.3f}")
    print(f"Golden expected reviews: {sum(golden.values()):,}\n")

    bench_dir = PROJECT_ROOT / "data" / "benchmark"
    bench_dir.mkdir(parents=True, exist_ok=True)

    results: list[dict] = []
    for n in worker_counts:
        pass_dir = bench_dir / f"workers_{n}"
        if pass_dir.exists():
            shutil.rmtree(pass_dir)
        pass_dir.mkdir(parents=True)
        output_csv = pass_dir / RAW_OUTPUT_FILENAME
        completed_path = pass_dir / COMPLETED_PLACES_FILENAME

        print(f"{'=' * 60}\n  [{n} workers]  starting…\n{'=' * 60}")
        run = await _run_workers(places, n, args.max_reviews, output_csv, completed_path)
        scraped = count_reviews_by_place(output_csv)
        recall = _compute_recall(
            scraped, {p["place_id"]: golden[p["place_id"]] for p in places}
        )
        failed = sum(1 for r in recall["recall_per_place"] if r < args.min_recall)
        results.append({
            "workers": n,
            "elapsed_s": run.elapsed_s,
            "places": len(places),
            "errors_open": run.errors,
            "reviews_emitted": run.reviews_emitted,
            "throughput_pl_s": len(places) / max(run.elapsed_s, 0.01),
            "mean_recall": recall["mean_recall"],
            "min_recall": recall["min_recall"],
            "places_perfect": recall["places_perfect"],
            "places_failed": failed,
            "places_over_expected": recall["places_over_expected"],
        })
        print(
            f"  [{n}w]  done in {run.elapsed_s:.1f}s | "
            f"mean_recall={recall['mean_recall']:.3f} "
            f"min={recall['min_recall']:.3f} "
            f"perfect={recall['places_perfect']}/{len(places)} "
            f"failed={failed}\n"
        )

    _print_table(results)
    _print_verdict(results, args.min_recall)
    _save_report(bench_dir, results, golden, places, args.min_recall)
    return 0


def _print_table(results: list[dict]) -> None:
    print("=" * 110)
    print("BENCHMARK RESULTS")
    print("=" * 110)
    header = ("Workers", "Time(s)", "Throughput", "MeanRecall", "MinRecall",
              "Perfect", "Failed", "Reviews", "Errors")
    print("  ".join(f"{h:>10}" for h in header))
    print("-" * 110)
    for r in results:
        print("  ".join((
            f"{r['workers']:>10}",
            f"{r['elapsed_s']:>10.1f}",
            f"{r['throughput_pl_s']:>10.2f}",
            f"{r['mean_recall']:>10.3f}",
            f"{r['min_recall']:>10.3f}",
            f"{r['places_perfect']:>10}",
            f"{r['places_failed']:>10}",
            f"{r['reviews_emitted']:>10}",
            f"{r['errors_open']:>10}",
        )))
    print("=" * 110)


def _print_verdict(results: list[dict], min_recall: float) -> None:
    min_per_place = min_recall * 0.95
    eligible = [
        r for r in results
        if r["mean_recall"] >= min_recall and r["min_recall"] >= min_per_place
    ]
    if not eligible:
        print(
            f"\n[VERDICT] No config met mean_recall ≥ {min_recall:.3f} "
            f"AND min_recall ≥ {min_per_place:.3f}.\n"
            f"          → Investigate worker.py before tuning concurrency."
        )
        return
    best = min(eligible, key=lambda r: r["elapsed_s"])
    print(
        f"\n[VERDICT] Optimal workers: {best['workers']} "
        f"(time {best['elapsed_s']:.1f}s, "
        f"mean_recall {best['mean_recall']:.3f}, "
        f"min_recall {best['min_recall']:.3f})"
    )


def _save_report(
    bench_dir: Path, results: list[dict], golden: dict,
    places: list[dict], min_recall: float,
) -> None:
    payload = {
        "min_recall_threshold": min_recall,
        "min_per_place_threshold": min_recall * 0.95,
        "results": results,
        "golden_size": len(golden),
        "sample_places": len(places),
    }
    report_path = bench_dir / "benchmark_report.json"
    report_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"\n[INFO] Report saved to {report_path}")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Recall-aware worker benchmark.")
    sub = parser.add_subparsers(dest="cmd", required=True)

    gg = sub.add_parser("generate-golden", help="Build ground-truth review counts.")
    gg.add_argument("--sample-csv", default="data/test/sample_places.csv")
    gg.add_argument("--golden-out", default="data/test/golden_reviews.json")
    gg.add_argument("--max-reviews", type=int, default=15_000)

    bn = sub.add_parser("bench", help="Run the recall-aware benchmark.")
    bn.add_argument("--sample-csv", default="data/test/sample_places.csv")
    bn.add_argument("--golden", default="data/test/golden_reviews.json")
    bn.add_argument("--configs", default="1,4,8")
    bn.add_argument("--max-reviews", type=int, default=15_000)
    bn.add_argument(
        "--min-recall", type=float, default=0.99,
        help="Mean per-place recall required for eligibility.",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    if args.cmd == "generate-golden":
        sys.exit(asyncio.run(cmd_generate_golden(args)))
    elif args.cmd == "bench":
        sys.exit(asyncio.run(cmd_bench(args)))


if __name__ == "__main__":
    main()
