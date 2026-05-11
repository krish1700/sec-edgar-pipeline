-- Silver: stg_companies
--
-- Flattens company metadata from the raw submissions feed.
-- One row per company — deduplicated to the most recently loaded record.
--
-- Key transforms:
--   - CIK zero-padded to 10-digit string
--   - Primary ticker and exchange extracted from JSON arrays
--   - SIC code cast to string for consistent joining

{{ config(materialized='table') }}

with raw as (
    select * from {{ ref('raw_submissions') }}
)

select
    lpad(cik_raw, 10, '0')                          as cik,
    company_name_raw                                 as company_name,
    sic_raw                                          as sic_code,
    sic_description_raw                              as sic_description,
    state_of_incorporation_raw                       as state_of_incorporation,
    -- tickers and exchanges are JSON arrays — extract the first (primary) value
    json_extract_string(tickers_raw,  '$[0]')        as primary_ticker,
    json_extract_string(exchanges_raw,'$[0]')        as primary_exchange,
    loaded_at

from raw
-- If the same company was loaded more than once, keep only the freshest record
qualify row_number() over (
    partition by cik_raw
    order by loaded_at desc
) = 1
