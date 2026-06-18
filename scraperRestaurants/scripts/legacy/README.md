# Legacy artifacts

Files in this folder are **not used by the live pipeline**. They are kept for
historical reference only.

| File | Why retired | Replaced by |
|---|---|---|
| `generate_queries.py` | Precomputed flat list of all `cat × district` queries to a 3 MB text file. | `orchestrator_peru.py` builds the queries in-memory per district loop. |
| `consultas_masivas_peru.txt` | Output of the above (committed at one point in time). | Generated on-demand if you really need it: `python scripts/legacy/generate_queries.py`. |

If you find yourself reaching for these files for production use, prefer
re-running the orchestrator — it is faster, supports auto-resume, and writes
per-district CSVs instead of a single monolithic file.
