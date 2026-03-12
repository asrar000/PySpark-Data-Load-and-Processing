# PySpark Property Search Pipeline

A PySpark ETL pipeline that joins property details and search data,
produces a standardized output, and writes a full validation report.

---

## Project Structure

```
PySpark-Data-Load-and-Processing/
├── data/
│   ├── property.json                      <- place your details input file here
│   ├── search.json                        <- place your search input file here
│   └── output/
│       ├── final_output/                  <- generated after run
│       └── unmatched_details/             <- generated after run
├── logs/
│   └── <YYMMDD>/
│       └── main_<YYMMDD>_<HHMMSS>.json   <- generated after run
├── .venv/                                 <- virtual environment (not committed)
├── main.py
├── config.py
├── test_main.py                           <- pytest unit tests
├── requirements.txt
├── pytest.ini
├── Dockerfile
├── docker-compose.yml
├── .dockerignore
├── .gitignore
├── validation_report.txt                  <- generated after run
└── README.md
```

---

## Input Files

Place your two input JSON files inside the `data/` folder:

```
data/property.json
data/search.json
```

The input file paths are configured in `config.py`:

```python
INPUT_DETAILS_FILE = "data/property.json"
INPUT_SEARCH_FILE  = "data/search.json"
```

If your file is named differently, update the path in `config.py` and no
changes are needed in `main.py`.

---

## Option A: Run With Docker

This is the recommended way to run the project. Docker handles Java, Python,
and all dependencies automatically — no local setup required.

### Prerequisites

Install Docker Desktop from https://www.docker.com/products/docker-desktop

Verify it is running:

```bash
docker --version
docker compose version
```

### 1. Place your input files

```bash
cp /path/to/property.json data/property.json
cp /path/to/search.json   data/search.json
```

### 2. Build the Docker image

```bash
docker compose build
```

This only needs to be done once, or when `requirements.txt` or `Dockerfile` changes.

### 3. Run the pipeline

```bash
docker compose up pipeline
```

Output files will be written to your local `data/output/` and `logs/` folders
because they are mounted as volumes.

### 4. Run the tests

```bash
docker compose up tests
```

### 5. Run both pipeline and tests

```bash
docker compose up
```

### 6. Clean up containers after use

```bash
docker compose down
```

---

## Option B: Run Locally Without Docker

### Prerequisites

#### Java

PySpark requires Java to be installed. Java 11, 17, or 21 are all supported.

```bash
sudo apt update
sudo apt install -y default-jdk
java -version
```

Set JAVA_HOME permanently:

```bash
echo 'export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))' >> ~/.bashrc
source ~/.bashrc
echo $JAVA_HOME
```

#### Python

Python 3.10 or higher is required.

```bash
python3 --version
```

### Setup

#### 1. Create a virtual environment

```bash
python3 -m venv .venv
```

#### 2. Activate the virtual environment

```bash
source .venv/bin/activate
```

You will see `(.venv)` appear at the start of your terminal prompt.

#### 3. Install dependencies

```bash
pip install -r requirements.txt
```

#### 4. Place input files

```bash
cp /path/to/property.json data/property.json
cp /path/to/search.json   data/search.json
```

### Run the pipeline

```bash
python3 main.py
```

---

## Testing

### Run all tests once (local)

```bash
pytest test_main.py -v
```

### Run all tests once (Docker)

```bash
docker compose up tests
```

### Run tests continuously during development

Open a second terminal, activate the virtual environment, then run:

```bash
ptw test_main.py -v
```

`ptw` watches all files and automatically reruns the tests every time you
save `test_main.py` or `main.py`.

### What the tests cover

| Function | What is tested |
|----------|----------------|
| `extract_details_fields` | Correct columns, uppercase country code, source_id cast to string |
| `extract_search_fields` | Correct columns, search_id cast to string |
| `search_quality_checks` | Missing URL count, missing price count, return type and keys |
| `drop_missing_source_id` | Null rows removed, dropped count correct, no nulls in result |
| `deduplicate` | Duplicates removed, count_before equals count_after when no dups |
| `build_matched_unmatched` | Matched count, unmatched count, extra id goes to unmatched |
| `make_slug` | Lowercase output, spaces replaced with dashes |
| `build_final_output` | 13 columns, GEN- prefix, published=True, price defaults, currency defaults, data_quality_flag |

---

## Output

| Path | Description |
|------|-------------|
| `data/output/final_output/` | Standardized JSON records (matched) |
| `data/output/unmatched_details/` | Details rows with no search match |
| `validation_report.txt` | Full pipeline statistics |
| `logs/<date>/main_<date>_<time>.json` | Structured JSON logs |

---

## Configuration

All parameters are defined in `config.py`. No values are hard-coded in `main.py`.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `INPUT_DETAILS_FILE` | `data/property.json` | Path to details input file |
| `INPUT_SEARCH_FILE` | `data/search.json` | Path to search input file |
| `JOIN_KEY` | `id` | Common key used to join both datasets |
| `DEFAULT_CURRENCY` | `USD` | Fallback currency if missing |
| `DEFAULT_USD_PRICE` | `0.0` | Fallback price if missing |
| `DEFAULT_STAR_RATING` | `0.0` | Fallback star rating if missing |
| `DEFAULT_REVIEW_SCORE` | `0.0` | Fallback review score if missing |
| `OUTPUT_FINAL_DIR` | `data/output/final_output` | Path for final output |
| `OUTPUT_UNMATCHED_DIR` | `data/output/unmatched_details` | Path for unmatched records |

---

## Output Fields

| Field | Rule |
|-------|------|
| `id` | Generated as GEN-source_id |
| `feed_provider_id` | Original source_id from details |
| `property_name` | English name from details |
| `property_slug` | Lowercase dash-separated from property_name |
| `country_code` | Trimmed uppercase from location |
| `currency` | USD if missing |
| `usd_price` | From price.book, default 0.0 |
| `star_rating` | From rating.stars, default 0.0 |
| `review_score` | From rating.review_score, default 0.0 |
| `commission` | commission.percentage from search |
| `meal_plan` | From first product meal_plan in search |
| `published` | Always true |
| `data_quality_flag` | GOOD or NEEDS_REVIEW based on missing fields (bonus) |

---

## Pipeline Steps

```
1.  Read property.json and search.json
2.  Extract required fields from details
3.  Extract required fields from search
4.  Run data quality checks on search
5.  Drop rows with missing source_id
6.  Deduplicate details on source_id
7.  Inner join  -> matched_details  (42 rows)
    Anti join   -> unmatched_details (57 rows)
8.  Build standardized final output from matched_details
9.  Write final_output and unmatched_details to disk
10. Write validation_report.txt
11. Write structured logs to logs/<date>/
```

---

## Every Time You Open a New Terminal (local)

```bash
source .venv/bin/activate
python3 main.py
```

To deactivate when you are done:

```bash
deactivate
```