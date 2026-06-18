"""Legacy: flat-file query generator. Superseded by orchestrator_peru.py."""

from __future__ import annotations

import csv
import sys
from pathlib import Path

# The legacy script expects to run from the config/ directory so that
# `from constant import categories` resolves.
CONFIG_DIR = Path(__file__).resolve().parent.parent.parent / "config"
sys.path.insert(0, str(CONFIG_DIR))

from constant import categories  # noqa: E402

INPUT_CSV = CONFIG_DIR / "geo_ref_pe.csv"
OUTPUT_TXT = Path(__file__).resolve().parent / "consultas_masivas_peru.txt"


def generate_queries() -> None:
    """Cross-product categories × districts into a single text file."""
    print(f"Loaded {len(categories)} categories")

    total = 0
    with open(INPUT_CSV, "r", encoding="utf-8") as fin, \
         open(OUTPUT_TXT, "w", encoding="utf-8") as fout:
        districts = list(csv.DictReader(fin))
        print(f"Loaded {len(districts)} districts")
        for cat in categories:
            for row in districts:
                fout.write(
                    f"{cat} en el distrito de {row['distrito']}, "
                    f"{row['provincia']}, {row['departamento']}, Perú\n"
                )
                total += 1

    print(f"Generated {total} queries → '{OUTPUT_TXT}'.")


if __name__ == "__main__":
    generate_queries()
