# SEC EDGAR Financial Filings Pipeline

End-to-end data engineering pipeline ingesting 10+ years of public company financials from the SEC EDGAR API — with GAAP normalization, amendment handling, and quarter-over-quarter anomaly detection.

```
SEC EDGAR API (data.sec.gov)
        │
        ▼
  ┌─────────────┐
  │   Airflow   │  Orchestration — daily/weekly DAGs with retry + XCom logging
  └──────┬──────┘
         │  Raw JSON (data/raw/)
         ▼
  ┌─────────────┐
  │   DuckDB    │  Bronze — raw landing tables, append-only, no transforms
  │   Bronze    │
  └──────┬──────┘
         │
         ▼
  ┌─────────────┐
  │   DuckDB    │  Silver — GAAP tag normalization, dedup, amendment handling
  │   Silver    │  (dbt)
  └──────┬──────┘
         │
         ▼
  ┌─────────────┐
  │   DuckDB    │  Gold — star schema + QoQ anomaly flags
  │    Gold     │  (dbt)
  └──────┬──────┘
         │
         ▼
  ┌──────────────┐
  │ Evidence.dev │  Live dashboard at localhost:3000
  └──────────────┘
```

---

## Table of Contents

- [Why This Project](#why-this-project)
- [Stack](#stack)
- [Key Technical Challenges Solved](#key-technical-challenges-solved)
- [Project Structure](#project-structure)
- [How to Run Locally](#how-to-run-locally)
- [Pipeline Phases](#pipeline-phases)
- [dbt Architecture](#dbt-architecture)
- [Anomaly Detection Logic](#anomaly-detection-logic)
- [Dashboard](#dashboard)
- [Data Quality](#data-quality)
- [Known Limitations](#known-limitations)
- [Roadmap](#roadmap)

---

## Why This Project

Most portfolio projects use clean, pre-packaged datasets. SEC EDGAR forces you to solve real engineering problems that come up at fintech companies, investment firms, and consulting shops:

- **Inconsistent GAAP taxonomy** — revenue may appear under `Revenues`, `RevenueFromContractWithCustomerExcludingAssessedTax`, or `SalesRevenueNet` depending on the company, industry, and filing year. No two 10-Ks are identical.
- **Filing amendments** — a 10-K/A supersedes its original. Naively loading all filings produces duplicate, contradictory rows.
- **Period overlap** — annual 10-K filings include quarterly data that also appears in 10-Q filings. Double-counting is silent and catastrophic for any metric.
- **Schema evolution** — GAAP concepts are added, deprecated, and renamed across years. Historical backfills require handling tags that no longer exist.
- **Real scale** — 50 companies × 40 quarters × 6 concepts = ~12,000 rows at minimum. Scaled to S&P 500, that's ~2M rows in `fact_financials`.

---

## Stack

| Layer | Tool | Version | Notes |
|---|---|---|---|
| Orchestration | Apache Airflow | 2.8+ | Runs via Docker Compose |
| Warehouse | DuckDB | 0.10+ | File-based, no server needed |
| Transformation | dbt-core + dbt-duckdb | latest | Medallion architecture |
| Serving / BI | Evidence.dev | latest | Markdown-based, connects directly to DuckDB |
| Language | Python | 3.11+ | Ingestion scripts |
| Containerization | Docker Desktop | latest | Required for Airflow |

**Data source:** [`data.sec.gov`](https://data.sec.gov) — free public API, no authentication required.

---

## Key Technical Challenges Solved

**1. GAAP tag normalization**
A `gaap_tag_synonyms.csv` seed maps 20+ synonym tags to canonical names. A `normalize_gaap_tag` dbt macro applies the mapping in the Silver layer. This means downstream models only ever see `Revenues`, `NetIncomeLoss`, `Assets` — never their company-specific variants.

**2. Filing amendment handling**
For every (company, concept, period) combination, only the row with the latest `filed_date` is kept. Implemented with `ROW_NUMBER() OVER (PARTITION BY cik, concept, period_end ORDER BY filed_date DESC)` in `stg_financials.sql`.

**3. 10-K / 10-Q period deduplication**
Annual 10-K filings contain data for Q4 and the full year. 10-Q filings cover Q1–Q3. The Silver model keeps `10-Q` rows for Q1/Q2/Q3 periods and `10-K` rows for Q4 and annual periods — never mixing form types for the same period.

**4. Instantaneous vs. period GAAP values**
Balance sheet items (`Assets`, `Liabilities`) carry a single `end` date. Income statement items (`Revenue`, `NetIncome`) carry both `start` and `end` dates. The staging model handles both correctly and assigns a `value_type` column (`instant` vs `duration`) for downstream filtering.

**5. CIK zero-padding**
SEC CIK numbers are 10-digit zero-padded strings in the API (`0000320193`) but are often stored as integers in seed files. All CIKs are standardized to zero-padded strings at ingestion time.

---

## Project Structure

```
sec-edgar-pipeline/
├── airflow/
│   ├── dags/
│   │   ├── ingest_company_facts.py     # Daily DAG: fetch XBRL facts per CIK
│   │   ├── ingest_submissions.py       # Weekly DAG: refresh company metadata
│   │   └── run_dbt_transformations.py  # Trigger dbt after ingestion completes
│   └── plugins/
│       └── sec_api_hook.py             # Reusable hook for SEC API calls
├── ingestion/
│   ├── fetch_company_facts.py          # Core XBRL facts fetch logic
│   ├── fetch_submissions.py            # Filing metadata fetch
│   ├── companies.csv                   # Seed list of CIK numbers to track
│   └── utils.py                        # Rate limiter, retry logic, logging
├── dbt/
│   ├── models/
│   │   ├── bronze/
│   │   │   ├── raw_company_facts.sql
│   │   │   ├── raw_submissions.sql
│   │   │   └── schema.yml
│   │   ├── silver/
│   │   │   ├── stg_financials.sql      # GAAP tag normalization + dedup
│   │   │   ├── stg_companies.sql       # Company metadata standardization
│   │   │   ├── stg_filing_periods.sql  # Period parsing + gap detection
│   │   │   └── schema.yml
│   │   └── gold/
│   │       ├── fact_financials.sql     # Core fact table
│   │       ├── dim_companies.sql
│   │       ├── dim_fiscal_periods.sql
│   │       ├── dim_gaap_concepts.sql
│   │       ├── rpt_anomaly_flags.sql   # QoQ anomaly detection output
│   │       └── schema.yml
│   ├── tests/
│   │   ├── assert_no_duplicate_periods.sql
│   │   └── assert_revenue_nonnegative.sql
│   ├── macros/
│   │   ├── normalize_gaap_tag.sql      # Maps synonym tags to canonical names
│   │   └── fiscal_quarter.sql          # Extracts quarter from period string
│   ├── seeds/
│   │   └── gaap_tag_synonyms.csv       # Lookup table for tag normalization
│   ├── dbt_project.yml
│   └── profiles.yml                    # DuckDB connection config (not committed)
├── dashboard/
│   ├── pages/
│   │   ├── index.md                    # Company overview + summary cards
│   │   ├── financials.md               # Revenue / income trend charts
│   │   ├── anomalies.md                # Flagged QoQ outliers
│   │   └── comparisons.md              # Cross-company peer benchmarking
│   └── evidence.config.yaml
├── data/
│   └── raw/
│       ├── submissions/                # Landing zone: {cik}.json
│       └── facts/                      # Landing zone: {cik}.json (5–20MB each)
├── warehouse/
│   └── edgar.duckdb                    # Local DuckDB warehouse file
├── .env.example                        # Required environment variables template
├── docker-compose.yml                  # Airflow local setup
├── requirements.txt
└── README.md
```

---

## How to Run Locally

### Prerequisites

- Docker Desktop running
- Python 3.11+
- Node.js 18+ (for Evidence.dev)

### 1. Clone and configure environment

```bash
git clone https://github.com/your-username/sec-edgar-pipeline.git
cd sec-edgar-pipeline
cp .env.example .env
# Edit .env — set SEC_USER_AGENT and DUCKDB_PATH
```

### 2. Start Airflow

```bash
docker-compose up -d
# Airflow UI available at http://localhost:8080
# Default credentials: airflow / airflow
```

### 3. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 4. Run initial backfill (one-time)

```bash
python ingestion/fetch_submissions.py --all
python ingestion/fetch_company_facts.py --all
```

### 5. Run dbt transformations

```bash
cd dbt
dbt deps
dbt seed                  # Load gaap_tag_synonyms.csv
dbt run                   # Bronze → Silver → Gold
dbt test                  # Run all data quality tests
dbt docs generate
dbt docs serve            # Lineage graph at http://localhost:8080
```

### 6. Start Evidence.dev dashboard

```bash
cd dashboard
npm install
npm run dev               # Dashboard at http://localhost:3000
```

---

## Pipeline Phases

| Phase | Days | Deliverable |
|---|---|---|
| 1 — Environment Setup | 1–2 | All tools running locally |
| 2 — Ingestion Layer | 3–7 | Raw JSON landing in `data/raw/` on Airflow schedule |
| 3 — Bronze & Silver | 8–14 | `dbt test` green; `stg_financials` deduplicated |
| 4 — Gold + Anomaly | 15–19 | `rpt_anomaly_flags` returning results for 3+ companies |
| 5 — dbt Quality | 20–22 | Full test coverage + dbt docs published |
| 6 — Dashboard | 23–26 | All 4 Evidence.dev pages live at localhost:3000 |

---

## dbt Architecture

```
Bronze (raw landing)
    raw_company_facts        raw_submissions
           │                       │
           └──────────┬────────────┘
                       ▼
Silver (clean + standardized)
    stg_financials    stg_companies    stg_filing_periods
           │                │                  │
           └────────────────┴──────────────────┘
                            ▼
Gold (star schema + analytics)
    dim_companies    dim_fiscal_periods    dim_gaap_concepts
           │                │                    │
           └────────────────┴────────────────────┘
                            ▼
                    fact_financials
                            │
                            ▼
                  rpt_anomaly_flags
```

### Key dbt models

| Model | Layer | Description |
|---|---|---|
| `raw_company_facts` | Bronze | Raw XBRL JSON loaded into DuckDB, no transforms |
| `raw_submissions` | Bronze | Raw filing metadata JSON, append-only |
| `stg_financials` | Silver | Unnested XBRL facts, GAAP tags normalized, duplicates removed |
| `stg_companies` | Silver | Standardized company metadata, CIK zero-padded |
| `stg_filing_periods` | Silver | Parsed fiscal periods, gap detection flags |
| `fact_financials` | Gold | Core fact table with surrogate keys and traceability columns |
| `dim_companies` | Gold | Company dimension with SIC code, exchange, active flag |
| `dim_fiscal_periods` | Gold | Fiscal/calendar period mapping |
| `dim_gaap_concepts` | Gold | Canonical GAAP concept metadata (seeded) |
| `rpt_anomaly_flags` | Gold | QoQ anomaly detection output |

---

## Anomaly Detection Logic

`rpt_anomaly_flags` flags a (company, concept, quarter) record as anomalous when any of the following conditions are true:

| Rule | Condition |
|---|---|
| Revenue spike | QoQ revenue change exceeds ±30% with no corresponding change in the prior-year same quarter |
| Income sign flip | Net income flips positive ↔ negative without a corresponding revenue decline |
| EPS drop | EPS drops more than 2 standard deviations below the trailing 8-quarter average |
| Asset jump | Total assets change by more than 40% QoQ (may indicate acquisition or divestiture) |
| Restatement | A previously reported value is restated by more than 10% in an amended filing |

Implemented using dbt window functions: `LAG()` for prior-period comparisons and `STDDEV_POP()` over an 8-quarter trailing window for the EPS rule.

---

## Dashboard

Built with [Evidence.dev](https://evidence.dev) — markdown files with embedded SQL queries, rendered as a live web app connected directly to the DuckDB warehouse.

| Page | Description |
|---|---|
| **Overview** (`index.md`) | Company selector, summary cards (Revenue, Net Income, EPS, Assets), 8-quarter sparklines |
| **Financials** (`financials.md`) | Revenue, Operating Income, Net Income trend lines; quarterly/annual toggle; YoY % change bars |
| **Anomalies** (`anomalies.md`) | All flagged QoQ anomalies sorted by severity; filter by company/concept/quarter; link to SEC filing accession |
| **Benchmarking** (`comparisons.md`) | Multi-company overlay by SIC code; normalized revenue growth and margin trends |

> **Note:** Evidence.dev connects to DuckDB in read-only mode (`read_only=true` in `evidence.config.yaml`) to avoid write lock conflicts with Airflow DAGs running simultaneously.

---

## Data Quality

dbt test coverage across all layers:

| Test Type | Examples |
|---|---|
| Generic — `not_null` | All surrogate keys, CIK, period end date |
| Generic — `unique` | `financial_key` in `fact_financials` |
| Generic — `relationships` | All FK columns reference their dimension tables |
| Generic — `accepted_values` | `form_type` in (`10-K`, `10-Q`), `value_type` in (`instant`, `duration`) |
| Singular — `assert_no_duplicate_periods` | No company has two rows for the same concept + period |
| Singular — `assert_revenue_nonnegative` | Revenue values are never negative (data quality flag) |
| Source freshness | Raw data must be refreshed within 25 hours — Airflow alerts on breach |

---

## Known Limitations

- **Stock split adjustment:** `normalized_value` in `fact_financials` currently handles restatements only. True split-adjusted values require an external source (e.g., yfinance) not yet integrated.
- **`is_active` flag:** Determined heuristically — companies with no filing activity in the last 18 months are marked inactive. EDGAR does not publish delisting events directly.
- **DuckDB concurrency:** DuckDB supports only one write connection. Airflow DAGs acquire a write lock during `dbt run`. Evidence.dev uses read-only mode. Do not run `dbt run` while a read-only session is open on the same file.
- **Company coverage:** Currently seeded with 50 companies across tech, healthcare, financials, and energy. Scale to S&P 500 is tested but not the default configuration.

---

## Roadmap

- [ ] ML-based anomaly scoring with scikit-learn isolation forest (replace rule-based flags)
- [ ] SEC 8-K event detection joined to anomaly flags (correlate financial anomalies with material disclosures)
- [ ] Incremental dbt models using DuckDB `MERGE` syntax (avoid full refreshes in production)
- [ ] GitHub Actions CI running `dbt test` on every push
- [ ] Scale ingestion to full S&P 500 (~500 companies, ~50M rows in `fact_financials`)

---

## References

- [SEC EDGAR Full-Text Search](https://efts.sec.gov) — find CIK numbers by company name
- [SEC EDGAR XBRL API docs](https://www.sec.gov/edgar/sec-api-documentation) — API endpoint reference
- [dbt-duckdb adapter](https://github.com/duckdb/dbt-duckdb) — connecting dbt to DuckDB
- [Evidence.dev documentation](https://docs.evidence.dev) — dashboard authoring guide
- [GAAP taxonomy browser](https://xbrl.fasb.org/us-gaap/) — look up canonical concept names
