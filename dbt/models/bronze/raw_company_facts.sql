-- Bronze: raw_company_facts
--
-- Reads all companyfacts JSON files from disk into DuckDB as-is.
-- No transformations — this is a faithful copy of what the SEC API returned.
--
-- Source files : data/raw/facts/{cik}.json  (one file per company, 5–20 MB each)
-- Configured in: dbt_project.yml  →  vars.raw_facts_path
--
-- The 'facts' column contains deeply nested XBRL data.
-- The Silver model (stg_financials) is responsible for unnesting it.
--
-- append-only: never update or delete rows from this table.

{{ config(materialized='table') }}

select
    -- CIK comes back as an integer from the API; cast to string for consistency.
    -- Zero-padding to 10 digits happens in the Silver layer.
    cast(cik as varchar)            as cik_raw,

    entityName                      as entity_name,

    -- Keep the full facts blob intact — Silver will unnest this.
    -- DuckDB stores it as a JSON column.
    facts                           as facts_blob,

    -- Record when this row was loaded so we can track freshness
    current_timestamp               as loaded_at

from read_json(
    '{{ var("raw_facts_path") }}',
    -- Specify columns explicitly so DuckDB reads 'facts' as an opaque JSON blob.
    -- Without this, DuckDB tries to infer a unified schema across all 54 files,
    -- which fails because each company has different GAAP concept keys inside 'facts'.
    columns = {
        'cik':        'BIGINT',
        'entityName': 'VARCHAR',
        'facts':      'JSON'
    },
    maximum_object_size = 33554432,
    -- When multiple files have different nested structures, merge by column name
    -- rather than by column position
    union_by_name = true
)
