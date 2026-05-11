{% macro normalize_gaap_tag(tag_column) %}
{#
  Maps a raw GAAP synonym tag to its canonical name using the
  gaap_tag_synonyms seed table.

  If the tag is not found in the seed, the original value is returned
  unchanged — so no data is silently dropped for unknown concepts.

  Usage:
    {{ normalize_gaap_tag('raw_concept') }}
#}
coalesce(
    (
        select s.canonical_tag
        from {{ ref('gaap_tag_synonyms') }} s
        where s.synonym_tag = {{ tag_column }}
        limit 1
    ),
    {{ tag_column }}
)
{% endmacro %}
