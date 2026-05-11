-- Silver: stg_financials
--
-- The hardest model in the project. Transforms the raw XBRL JSON blob into
-- clean, flat, deduplicated financial fact rows.
--
-- Five engineering problems solved here:
--   1. Unnesting   — the JSON is 3 levels deep before we reach individual values
--   2. Tag synonyms — Revenue has 6+ different concept names across companies
--   3. Amendments  — 10-K/A filings supersede the original; keep only the latest
--   4. Period overlap — 10-K and 10-Q both report Q4 data; keep one source
--   5. CIK format  — standardize to 10-digit zero-padded string
--
-- DuckDB JSON limitation: dynamic key lookup (-> column_name) and dynamic path
-- arrays (json_extract(blob, computed_array)) are not supported. The only safe
-- approach is literal string keys. We use dbt run_query at compile time to read
-- the synonym list from the seed and generate a UNION ALL with literal paths.

{{ config(materialized='table') }}

-- ── Compile-time: fetch the synonym list from the seed ────────────────────────
{% set concept_query %}
    select distinct synonym_tag
    from {{ ref('gaap_tag_synonyms') }}
    order by synonym_tag
{% endset %}

{% if execute %}
    {% set results = run_query(concept_query) %}
    {% set synonym_tags = results.columns['synonym_tag'].values() %}
{% else %}
    {# During dbt parse (no execution), use an empty list to avoid errors #}
    {% set synonym_tags = [] %}
{% endif %}

with

-- ── Step 1: Base table with CIK and us-gaap blob ──────────────────────────────
bronze as (
    select
        lpad(cik_raw, 10, '0')             as cik,
        entity_name,
        facts_blob -> 'us-gaap'            as us_gaap_blob
    from {{ ref('raw_company_facts') }}
    where (facts_blob -> 'us-gaap') is not null
),

-- ── Step 2: Extract USD arrays for each tracked concept ───────────────────────
-- run_query above gives us the synonym list at compile time.
-- dbt renders one SELECT per synonym tag — all keys are string literals,
-- which DuckDB's -> operator supports safely.
-- The UNION ALL is fully resolved before DuckDB sees the SQL.
tracked_concepts as (
    {% for tag in synonym_tags %}
    select
        cik,
        entity_name,
        '{{ tag }}'                                              as concept_name,
        (us_gaap_blob -> '{{ tag }}' -> 'units' -> 'USD')::json[] as usd_array
    from bronze
    where (us_gaap_blob -> '{{ tag }}' -> 'units' -> 'USD') is not null
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
    {% if synonym_tags | length == 0 %}
    select null::varchar as cik, null::varchar as entity_name,
           null::varchar as concept_name, null::json[] as usd_array
    where false
    {% endif %}
),

-- ── Step 3: Unnest the USD filing entries array ────────────────────────────────
unnested as (
    select
        cik,
        entity_name,
        concept_name                        as raw_concept,
        entry
    from tracked_concepts,
        unnest(usd_array)                   as t(entry)
),

-- ── Step 5: Parse JSON fields into typed columns ──────────────────────────────
-- ->> extracts a JSON field as text; try_cast handles malformed dates safely.
extracted as (
    select
        cik,
        entity_name,
        raw_concept,
        {{ normalize_gaap_tag('raw_concept') }}         as canonical_concept,
        try_cast(entry ->> 'val'    as double)          as reported_value,
        try_cast(entry ->> 'start'  as date)            as period_start,
        try_cast(entry ->> 'end'    as date)            as period_end,
        (entry ->> 'accn')                              as accession_number,
        upper(entry ->> 'form')                         as form_type,
        try_cast(entry ->> 'filed'  as date)            as filed_date,
        (entry ->> 'fp')                                as fiscal_period_label,
        -- Balance sheet items have no start date (point-in-time snapshot)
        -- Income statement items have both start and end (flow over a period)
        case
            when (entry ->> 'start') is null then 'instant'
            else 'duration'
        end                                             as value_type
    from unnested
    where
        try_cast(entry ->> 'val'   as double) is not null
        and try_cast(entry ->> 'end' as date) is not null
        -- Exclude 8-K (material event disclosures) — they contain preliminary
        -- earnings data that is always superseded by the 10-K or 10-Q filing.
        -- Keeping them would create duplicates and inflate row counts.
        and upper(entry ->> 'form') not in ('8-K', '8-K/A')
),

-- ── Step 6: Remove duplicate amendments ──────────────────────────────────────
-- A 10-K/A (amendment) supersedes the original 10-K for the same period.
-- Keep only the row with the latest filed_date per
-- (company, concept, period_end, form_type).
-- ROW_NUMBER = 1 is the most recently filed version.
deduped_amendments as (
    select
        *,
        row_number() over (
            partition by cik, canonical_concept, period_end, form_type
            order by filed_date desc nulls last
        ) as rn
    from extracted
),

latest_only as (
    select * exclude (rn)
    from deduped_amendments
    where rn = 1
),

-- ── Step 7: Remove 10-K / 10-Q period overlap ─────────────────────────────────
-- Annual 10-K filings contain Q4 and full-year data that also appears in 10-Qs.
-- Rule: Q1/Q2/Q3 → prefer 10-Q.  Q4 and FY → prefer 10-K.
-- ROW_NUMBER = 1 picks the preferred form type for each period.
form_deduped as (
    select
        *,
        {{ fiscal_quarter('period_end') }}              as calendar_quarter,
        year(period_end)                                as period_year,
        row_number() over (
            partition by cik, canonical_concept, period_end
            order by
                case
                    when {{ fiscal_quarter('period_end') }} in (1, 2, 3)
                         and form_type = '10-Q'         then 1
                    when {{ fiscal_quarter('period_end') }} = 4
                         and form_type = '10-K'         then 1
                    when fiscal_period_label = 'FY'
                         and form_type = '10-K'         then 1
                    else 2
                end
        ) as form_rn
    from latest_only
)

select * exclude (form_rn)
from form_deduped
where form_rn = 1
