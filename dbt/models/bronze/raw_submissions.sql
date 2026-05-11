-- Bronze: raw_submissions
--
-- Reads all submission metadata JSON files from disk into DuckDB as-is.
-- No transformations — faithful copy of the SEC submissions endpoint response.
--
-- Source files : data/raw/submissions/{cik}.json  (one file per company, ~50 KB each)
-- Configured in: dbt_project.yml  →  vars.raw_submissions_path
--
-- The 'filings' column contains nested filing history arrays.
-- The Silver model (stg_companies) extracts the top-level company metadata.
--
-- append-only: never update or delete rows from this table.

{{ config(materialized='table') }}

select
    cast(cik as varchar)            as cik_raw,
    name                            as company_name_raw,

    -- Industry classification code (4-digit integer)
    cast(sic as varchar)            as sic_raw,
    sicDescription                  as sic_description_raw,

    stateOfIncorporation            as state_of_incorporation_raw,

    -- Arrays of tickers and exchanges — a company can have multiple
    -- The Silver layer extracts the primary (first) ticker and exchange
    tickers                         as tickers_raw,
    exchanges                       as exchanges_raw,

    -- Full filing history — kept as blob for Silver to flatten
    filings                         as filings_blob,

    current_timestamp               as loaded_at

from read_json_auto(
    '{{ var("raw_submissions_path") }}',
    auto_detect = true
)
