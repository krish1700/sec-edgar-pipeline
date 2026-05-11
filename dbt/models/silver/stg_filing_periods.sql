-- Silver: stg_filing_periods
--
-- Summarises the filing period coverage for each company.
-- Detects gaps — quarters where a company has no reported data —
-- which can indicate late filings, restatements, or data quality issues.
--
-- One row per company with period coverage statistics.
-- The Gold layer uses this to annotate fact_financials with gap flags.

{{ config(materialized='table') }}

with

-- Get all distinct (company, quarter-end) combinations that actually exist
actual_periods as (
    select
        cik,
        period_end,
        {{ fiscal_quarter('period_end') }}      as calendar_quarter,
        year(period_end)                         as period_year
    from {{ ref('stg_financials') }}
    -- Use only one canonical concept to count periods — avoids inflating counts
    where canonical_concept = 'Revenues'
),

-- Summarise coverage per company
company_coverage as (
    select
        cik,
        min(period_end)                          as first_period,
        max(period_end)                          as last_period,
        count(distinct period_end)               as actual_quarters,
        -- Expected number of quarters from first to last filing
        datediff('quarter', min(period_end), max(period_end)) + 1
                                                 as expected_quarters
    from actual_periods
    group by cik
)

select
    c.cik,
    c.first_period,
    c.last_period,
    c.actual_quarters,
    c.expected_quarters,
    -- Negative means missing quarters; zero means complete coverage
    c.expected_quarters - c.actual_quarters      as missing_quarters,
    case
        when c.expected_quarters > c.actual_quarters then true
        else false
    end                                          as has_gaps
from company_coverage c
