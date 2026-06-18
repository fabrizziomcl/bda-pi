"""Fetch Peruvian Ubigeo data and emit `geo_ref_pe.csv` + hierarchical JSON."""

from __future__ import annotations

import json

import pandas as pd
import requests

UBIGEO_BASE = "https://raw.githubusercontent.com/ernestorivero/Ubigeo-Peru/master/json"
URL_DEPT = f"{UBIGEO_BASE}/ubigeo_peru_2016_departamentos.json"
URL_PROV = f"{UBIGEO_BASE}/ubigeo_peru_2016_provincias.json"
URL_DIST = f"{UBIGEO_BASE}/ubigeo_peru_2016_distritos.json"


def main() -> None:
    print("Downloading UBIGEO datasets...")
    dept = pd.DataFrame(requests.get(URL_DEPT, timeout=30).json()).rename(
        columns={"id": "department_id", "name": "departamento"}
    )
    prov = pd.DataFrame(requests.get(URL_PROV, timeout=30).json()).rename(
        columns={"id": "province_id", "name": "provincia"}
    )
    dist = pd.DataFrame(requests.get(URL_DIST, timeout=30).json()).rename(
        columns={"id": "ubigeo", "name": "distrito"}
    )

    print("Merging relational tables...")
    df = dist.merge(prov[["province_id", "provincia"]], on="province_id", how="left")
    df = df.merge(dept[["department_id", "departamento"]], on="department_id", how="left")
    df = df[["ubigeo", "departamento", "provincia", "distrito"]].sort_values(
        by=["departamento", "provincia", "distrito"]
    )

    df.to_csv("geo_ref_pe.csv", index=False, encoding="utf-8")

    hierarchy: dict[str, dict[str, list[str]]] = {}
    for _, row in df.iterrows():
        hierarchy.setdefault(row["departamento"], {}).setdefault(
            row["provincia"], []
        ).append(row["distrito"])

    with open("diccionario_distritos_peru.json", "w", encoding="utf-8") as f:
        json.dump(hierarchy, f, ensure_ascii=False, indent=4)

    print(f"Processed {len(df)} districts.")
    print("- Generated geo_ref_pe.csv")
    print("- Generated diccionario_distritos_peru.json")


if __name__ == "__main__":
    main()
