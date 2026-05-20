{#
  pii_hash(col_expr)
  ─────────────────────────────────────────────────────────────────────────────
  Returns a deterministic non-negative INT64 surrogate key for a PII column.

  SECURITY DESIGN NOTE:
  col_expr is a SQL column reference (e.g. 'usr_id') authored by dbt model
  developers in this repository — it is never derived from end-user runtime
  input. dbt macros are Jinja templates resolved at compile time, not at query
  execution time, so there is no runtime injection surface. The ~ operator
  performs Jinja string concatenation at compile time to build the SQL
  expression that Trino will execute.

  Namespace prefix:
  A 'pii:' prefix is prepended so that the same raw value in different
  semantic contexts (usr_id vs. gd_id) produces distinct surrogate keys,
  preventing cross-domain hash correlation.

  NULL safety:
  This macro handles NULL inline via COALESCE before passing the composed
  expression to stable_hash64_number. The sentinel value '__null__' produces
  a stable, distinct hash for NULL inputs rather than propagating NULL.
  stable_hash64_number also contains its own COALESCE as defence-in-depth for
  direct callers, but pii_hash does not rely on it.

  Usage example:
    {{ pii_hash('usr_id') }} as usr_pk
#}
{% macro pii_hash(col_expr) %}
  {{ stable_hash64_number("'pii:' || coalesce(cast(" ~ col_expr ~ " as varchar), '__null__')") }}
{% endmacro %}
