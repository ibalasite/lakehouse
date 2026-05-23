{#
  stable_hash64_number(input_expr)
  ─────────────────────────────────────────────────────────────────────────────
  Returns a deterministic non-negative INT64 hash of any scalar expression.

  Implementation notes:
  • input_expr is a trusted SQL expression assembled by dbt macros in this
    project — never sourced from raw user input — so direct interpolation is
    intentional and consistent with standard dbt macro patterns.
  • NULL safety: COALESCE maps NULL to the sentinel '__NULL__' (uppercase per
    EDD §9.4) before hashing so that NULL inputs produce a stable, distinct hash
    instead of propagating NULL downstream.
  • 9223372036854775807 = 2^63 - 1 (max signed INT64). The bitwise_and strips
    the sign bit produced by xxhash64, guaranteeing a non-negative result that
    is safe to store in BIGINT / INT64 columns without overflow.
#}
{% macro stable_hash64_number(input_expr) %}
  bitwise_and(
    from_big_endian_64(xxhash64(to_utf8(cast(coalesce(cast({{ input_expr }} as varchar), '__NULL__') as varchar)))),
    9223372036854775807
  )
{% endmacro %}
