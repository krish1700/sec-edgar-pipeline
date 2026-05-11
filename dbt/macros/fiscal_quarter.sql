{% macro fiscal_quarter(date_column) %}
{#
  Returns the calendar quarter number (1–4) for a given date column.

  Used in stg_financials to:
    1. Classify periods as Q1/Q2/Q3 vs Q4 for 10-K/10-Q deduplication
    2. Detect which form type is preferred for each period

  Usage:
    {{ fiscal_quarter('period_end') }}
#}
ceil(month({{ date_column }}) / 3.0)::integer
{% endmacro %}
