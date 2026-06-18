# Google Maps Reviews Scraper (Async Playwright Edition)

Scraper basado en **Playwright Async** que extrae reseñas de Google Maps a alta escala. Un único navegador Chromium con N `BrowserContext` aislados consume una `asyncio.Queue` de lugares; los resultados se escriben en un CSV append-only con `asyncio.Lock`.

## 🗺️ Project Scope

Este repositorio es la **Fase 2** del pipeline gastronómico BDA. Toma el output de Fase 1 ([`mapScraper`](../mapScraper)) y extrae reseñas de cada lugar. Es invocable directamente desde el monorepo (`bda_gmaps_restreviews`) o como submódulo independiente.

---

## ⚡ Características principales

1. **Async + 1 browser compartido:** El orquestador lanza **un solo `chromium.launch`** y crea N `BrowserContext` (uno por worker async). Cada worker consume de la misma cola. El paralelismo es por cooperativa (asyncio), no por proceso — no hay multiprocessing real, lo cual mantiene el footprint de RAM mucho más bajo que N navegadores.
2. **Anti-bot básico:** El `setup_context` sobreescribe `navigator.webdriver`, `hardwareConcurrency` y `platform` antes de cualquier request. Soporta XPATH multilingüe (ES/EN) para el tab de Opiniones.
3. **Resume con razón:** `completed_places.txt` registra `place_id,scraped_count,reason,timestamp` (post-audit C3). Solo las razones `ok` y `no_reviews_tab` son terminales; el orquestador acepta `--retry-non-terminal` para reintentar `dom_stable`/`timeout`/`error`.
4. **Atomicidad:** Los appends al CSV principal van con `flush + fsync` bajo lock; el monitor escribe state JSON con write-then-rename.
5. **Benchmark recall-aware:** `utils/benchmark_workers.py` elige el número óptimo de workers bajo `--min-recall ≥ 0.99` contra un golden dataset generado con un solo worker, no por throughput puro.

---

## 📋 Requisitos

| Requirement | Versión |
|---|---|
| Python | ≥ 3.9 |
| SO | Windows, Linux o macOS |

*(No necesitas descargar ChromeDriver, Playwright gestiona sus propios navegadores).*

---

## 📦 Instalación

```bash
git clone https://github.com/christivn/googlemaps-reviews-scraper-es.git
cd googlemaps-reviews-scraper-es

# Crear entorno virtual con Conda (Python 3.11 recomendado)
conda create -n reviews-scraper python=3.11 -y

# Activar entorno
conda activate reviews-scraper

# Instalar dependencias
pip install -r requirements.txt

# Descargar los navegadores de Playwright (CRÍTICO)
playwright install chromium
```

---

## 🚀 Uso: Raspado masivo (Orquestador)

Un único Chromium se lanza al arranque; los N workers son coroutines compartiendo ese browser.

```bash
# 4 workers, cap 15k reseñas por place
python orchestrator.py --input data/input/places_peru.csv --workers 4 --max-reviews 15000

# 12 workers, sin cap (ideal para alta-recall en restaurantes top con >15k reseñas)
python orchestrator.py --input data/input/places_peru.csv --workers 12 --max-reviews 0

# Reanudar e incluir lugares previamente marcados dom_stable/timeout/error
python orchestrator.py --input data/input/places_peru.csv --workers 8 --retry-non-terminal
```

### Parámetros del orquestador
*   `--input`: CSV con al menos las columnas `id` y `url_place` (output de Fase 1).
*   `--workers`: Coroutines concurrentes. Sin proceso multipplexing — todos comparten un solo Chromium.
*   `--max-reviews`: Cap por lugar. `0` = ilimitado.
*   `--output-dir`: Destino del `reviews_raw.csv` append-only.
*   `--retry-non-terminal`: Reintenta lugares cuyo último intento terminó en `dom_stable`, `timeout` o `error`.
*   `--skip-etl`: Salta el ETL post-scraping.

---

## 🛠️ Herramientas de desarrollo

### Benchmark recall-aware

El benchmark **no decide por throughput** — eso premiaba configs que terminaban rápido perdiendo reseñas silenciosamente. En su lugar:

```bash
# Paso 1: generar golden dataset (1 worker, sin cap, lento pero confiable)
python utils/benchmark_workers.py generate-golden \
    --sample-csv data/test/sample_places.csv \
    --golden-out data/test/golden_reviews.json

# Paso 2: medir cada config contra el golden
python utils/benchmark_workers.py bench \
    --sample-csv data/test/sample_places.csv \
    --golden data/test/golden_reviews.json \
    --configs 1,4,8,12 \
    --min-recall 0.99
```

El "óptimo" es el config más rápido que cumple `mean_recall ≥ 0.99` **y** `min_per_place_recall ≥ 0.99 × 0.95` — para que un lugar fallado no se compense con el promedio.

### Monitor incremental (base para streaming)

```bash
# Una sola pasada, emitir reviews desde el 2026-05-01
python monitor.py --input data/input/places_peru.csv --from-date 2026-05-01 --once

# Daemon: poll cada 30 min, solo lo nuevo desde la última pasada
python monitor.py --input data/input/places_peru.csv --interval 1800
```

El sink por defecto escribe CSV (`monitor_reviews_new.csv`). Para conectar a Kafka, implementa `KafkaSink` con `.emit(review_dict)` y `.close()` (gancho dejado en [monitor.py](monitor.py)).
