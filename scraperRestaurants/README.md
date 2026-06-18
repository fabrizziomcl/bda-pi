![Header](github-header-image.png?raw=true)

# Google Maps Scraper

A powerful Python tool for scraping Google Maps local services data. Extract detailed information about businesses and places directly from Google Maps search results.

## 🗺️ Project Scope

This repository (**mapScraper**) represents **Phase 1** of our Gastronomic Big Data Pipeline. Its primary responsibility is the massive, country-wide ingestion and consolidation of restaurant entities (places) into a clean, deduplicated data layer (Parquet).

> **Looking for the reviews scraper?**
> For **Phase 2** (async Playwright extraction of user reviews from the collected restaurants), see the sibling submodule [`reviewsScraper`](../reviewsScraper).

## 🚀 Features

With the **Google Maps Scraper**, you can obtain detailed data about businesses and specific places on Google Maps, such as:

- **Place ID** - Unique identifier for the location
- **Place URL** - Direct Google Maps link
- **Place name** - Business or location name
- **Category** - Type of business/service
- **Full address** - Complete location address
- **Phone number** - Contact phone number
- **Associated domain and URL** - Business website information
- **Coordinates** - Latitude and longitude
- **Average star rating** - Customer rating
- **Customizable search parameters** - Language, country, result limit, and output filename

> **Note (April 2026):** The legacy `reviews` column (total review count) is no longer emitted — Google's `tbm=map` endpoint stopped returning it. A regression test in [`tests/test_reviews_column.py`](tests/test_reviews_column.py) guards this. Per-place review counts are obtained during Fase 2 (`reviewsScraper`).

## 📋 Prerequisites

- Python 3.7 or higher
- pip (Python package installer)

## 📦 Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/christivn/mapScraper.git
   cd mapScraper
   ```

2. **Create a Conda virtual environment (Python 3.11 recommended):**
   ```bash
   conda create -n map-scraper python=3.11 -y
   conda activate map-scraper
   ```

3. **Install required packages:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Verify installation:**
   ```bash
   python mapScraperX.py --help
   ```

## 🔧 Usage

### Basic Syntax
```bash
python mapScraperX.py "your search query" [options]
```

### Command Line Options

| Option | Description | Default | Example |
|--------|-------------|---------|---------|
| `query` | Search query (required) | - | `"restaurants in NYC"` |
| `--lang` | Language code | `en` | `--lang es` |
| `--country` | Country code | `us` | `--country fr` |
| `--limit` | Maximum results | No limit | `--limit 100` |
| `--output-file` | Output CSV file path | `data/output.csv` | `--output-file results.csv` |

### 💡 Usage Examples

#### Basic Search
```bash
# Search for gyms in Seville, Spain
python mapScraperX.py "Gym in Seville Spain"
```

#### Language and Country Specific Search
```bash
# Search for dentists in Madrid (Spanish language, Spain country)
python mapScraperX.py "dentistas en Madrid" --lang es --country es
```

#### Limited Results
```bash
# Get only 50 pizza places in Paris
python mapScraperX.py "pizzerias in Paris" --lang fr --country fr --limit 50
```

#### Custom Output File
```bash
# Save results to a custom file
python mapScraperX.py "coffee shops in London" --output-file "data/london_coffee.csv"
```

#### Complex Query with All Options
```bash
# Comprehensive search with all parameters
python mapScraperX.py "barber shops in Tokyo" --lang ja --country jp --limit 25 --output-file "data/tokyo_barbers.csv"
```
### Complex query using file (for multiple queries)
```bash
# Comprehensive search using query list
python mapScraperX.py --queries-file qwuery_example.txt --lang ja --country jp --limit 25 --output-file "data/custom_name.csv"
```

### Concurrent query processing
```bash
# When requesting for more than one query (safe):
python mapScraperX.py --queries-file qwuery_example.txt --lang en --country jp --limit 25 --output-file "data/custom_name.csv" --concurrent 2
```

```bash
# When requesting for more than one query (fast but risky):
python mapScraperX.py --queries-file qwuery_example.txt --lang en --country jp --limit 25 --output-file "data/custom_name.csv" --concurrent 5
```


## 🌍 Supported Languages and Countries

### Common Language Codes
- `en` - English
- `es` - Spanish
- `fr` - French
- `de` - German
- `it` - Italian
- `pt` - Portuguese
- `ja` - Japanese
- `ko` - Korean
- `zh` - Chinese

### Common Country Codes
- `us` - United States
- `gb` - United Kingdom
- `es` - Spain
- `fr` - France
- `de` - Germany
- `it` - Italy
- `jp` - Japan
- `ca` - Canada
- `au` - Australia

## 📁 Output Format

The scraper generates a CSV file with the following columns:

| Column | Description | Example |
|--------|-------------|---------|
| `id` | Google Place ID | `ChIJN1t_tDeuEmsRUsoyG83frY4` |
| `url_place` | Direct Google Maps link | `https://www.google.com/maps/place/?q=place_id:...` |
| `title` | Business name | `Joe's Pizza` |
| `category` | Business category | `Pizza restaurant` |
| `address` | Full address | `123 Main St, New York, NY 10001` |
| `phoneNumber` | Local phone format | `(555) 123-4567` |
| `completePhoneNumber` | International format | `+1 555-123-4567` |
| `domain` | Website domain | `joespizza.com` |
| `url` | Full website URL | `https://www.joespizza.com` |
| `coor` | Coordinates (lat,lng) | `40.7128,-74.0060` |
| `stars` | Average rating | `4.5` |
| `source_query` | Original query that produced this row | `Pollería en el distrito de ...` |

## 🤖 Massive Geographic Orchestrator

For large-scale country-wide extractions (e.g., scanning multiple categories across thousands of districts without triggering anti-bot bans or losing data due to crashes), a geographic orchestrator script is provided.

The `orchestrator_peru.py` automatically reads the geographical references in `config/geo_ref_pe.csv`, executes multiple concurrent category queries for each district (defined in `config/constant.py`), and saves the output in a neat, hierarchical folder structure: `data/Departamento/Provincia/Distrito.csv`.

It fully supports **auto-resume**: if you kill the process or get network errors, re-run the same command and it picks up where it left off — districts with an existing CSV are skipped. CSVs are written atomically (`*.partial` → `os.replace`) so a half-written file is impossible. Each completed district also emits a `<district>.json` manifest with start/end timestamps, rows-per-category breakdown and the parameters used.

### Orchestrator Setup
1. Generate the geographical base and dictionaries (run once):
   ```bash
   cd config
   python get_dist.py
   cd ..
   ```
2. Check or edit your search categories in `config/constant.py`.

### Orchestrator Usage
```bash
# Basic run (Extracts everything infinitely! No limits per category, Concurrent requests: 3)
python orchestrator_peru.py

# Distributed workload: Filter by department using a JSON array (great for parallel teamwork)
python orchestrator_peru.py --deps '["Tumbes", "Piura", "Lambayeque"]'

# Run with stricter throttling if you encounter Google IP bans
python orchestrator_peru.py --concurrent 2 --limit 15

# Force resume from a specific index (e.g. index 500 in the list of 1800+ districts)
python orchestrator_peru.py --start-idx 500
```

## 📊 Data Preprocessing Pipeline

A modular ETL pipeline is provided in `etl/` for post-processing the raw scraped data. It consolidates thousands of district-level CSV files into clean, deduplicated, compressed Parquet files — one per department.

### Pipeline Features

- **Deduplication** — Removes duplicate records by Google Place ID (`id` column). This eliminates both intra-file duplicates (caused by pagination) and cross-district overlaps (same business appearing in neighboring district searches).
- **Schema Optimization** — Casts columns to native types (`Float32`, `Int32`, `Categorical`) to reduce memory footprint and improve analytical query performance.
- **ZSTD Compression** — Outputs Apache Parquet files with Zstandard compression (level 9), typically achieving **95-99%** size reduction compared to raw CSVs.
- **Compression Reporting** — Prints a summary table with per-department and aggregate statistics (input size, output size, reduction %, unique row count) and saves it to `etl_report.json`.

### Output Structure

Running the pipeline generates the following clean structure in `data_parquet/`:
```
data_parquet/
├── Peru/
│   ├── Perú.csv         # Consolidated, globally deduplicated fallback
│   └── Perú.parquet     # Consolidated, globally deduplicated data
├── regions/
│   ├── Amazonas.parquet # Department-level data
│   ├── Lima.parquet
│   └── ...
└── etl_report.json      # Final run metrics and size reductions
```

### Pipeline Architecture

```
etl/
├── __init__.py          # Package init
├── dedup.py             # Deduplication by Google Place ID
├── optimize.py          # Schema type casting
├── compress.py          # ZSTD Parquet writer
├── report.py            # Human-readable compression statistics
├── pipeline.py          # Main orchestrator (imports from above)
└── create_test_env.py   # Test sandbox generator
```

### Pipeline Usage

```bash
# Install additional dependencies (one-time)
pip install -r requirements.txt

# Create a test sandbox from existing data (copies 2 departments)
python -m etl.create_test_env --src data --dest data_test --num-deps 2

# Run the pipeline against test data
python -m etl.pipeline --input-dir data_test --output-dir data_parquet

# Run the pipeline against all production data
python -m etl.pipeline --input-dir data --output-dir data_parquet
```

### Example Output
```
================================================================================
ETL PIPELINE
  Input:       /path/to/data_test
  Output:      /path/to/data_parquet
  Departments: 1
  Engine:      Polars (multithreaded)
================================================================================
  [DONE]   Amazonas                  | CSV:   14.58 MB -> Parquet:    68.3 KB | Reduction:  99.5% | Unique rows: 697 (0.08s)

  [INFO] Consolidating all regions into Perú.csv and Perú.parquet...

================================================================================
SUMMARY
  Departments processed: 1
  Total raw records:                 48,560
  Unique records (Department level): 697
  Total unique records (Perú):       697
  Raw CSV size (all source files):   14.58 MB
  Final Perú.csv size:               216.0 KB
  Final Perú.parquet size:           68.4 KB
  Final size reduction:              99.54%
  Total time:                        0.13s
================================================================================
  [INFO] Report saved to /path/to/data_parquet/etl_report.json
```

## 🔧 What Changed (April 2026 Fix)

Google permanently shut down the `/localservices/prolist` endpoint that this
scraper originally used (it now returns **HTTP 410 Gone**).

**What was changed:**
- The scraper no longer targets `/localservices/prolist`. It now uses a
  two-step approach:
  1. `GET https://www.google.com/maps/search/{query}` — fetches the Maps SPA
     page to extract an embedded canonical `pb=` search URL from the `<link>`
     tag in `<head>`.
  2. `GET https://www.google.com/search?tbm=map&...&pb=...` — fetches a
     `)]}'`-prefixed JSON payload that contains the actual search results in a
     nested array at `data[64]`.
- JavaScript rendering via `requests-html` / pyppeteer is **no longer needed**.
  Both requests are plain HTTP GETs; this makes the scraper faster and removes
  a heavyweight dependency.
- `requests-html` has been removed from `requirements.txt`. Only `aiohttp` and
  `tqdm` are required now.
- All extraction failures now log explicit error messages so failures are never
  silent.
- **Pagination deduplication fix:** The pagination loop now tracks seen Place IDs
  with a `set()`. When Google returns a page with no new results (which happens
  when a query has fewer real results than the page size), the crawler stops
  immediately instead of looping indefinitely. This prevents the massive row
  duplication previously observed in rural districts.

**Known limitation:** The `tbm=map` JSON response does not include review
counts. Since the column would always be empty it was removed from the output
schema entirely (verified empirically over 407,436 rows). All other fields
(id, title, category, address, phone, website, coordinates, stars) are fully
populated. Per-place review counts come from Fase 2 (`reviewsScraper`).



## 🐛 Troubleshooting

### Common Issues

1. **Empty results / "Could not find pb= search URL"**
   - Google may be showing a consent or cookie wall for your IP/region.
   - Try setting `--lang` and `--country` to match your actual locale.
   - Check your internet connection.

2. **"data[64] is missing"**
   - Google may have updated the response structure again.
   - Open an issue with the raw response logged at DEBUG level:
     ```bash
     python -c "import logging; logging.basicConfig(level=logging.DEBUG); \
       import mapScraper.placesCrawlerV2 as c; c.search('test', 'en', 'us', 5)"
     ```

3. **Permission denied when creating output directory**
   - Ensure you have write permissions in the target directory.
   - Try running with appropriate permissions or change the output path.

## 📝 License

This project is provided as-is for educational and research purposes. Please respect Google's Terms of Service and use responsibly.
