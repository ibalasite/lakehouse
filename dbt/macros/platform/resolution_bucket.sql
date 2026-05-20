{#
  resolution_bucket(hours_col)
  ─────────────────────────────────────────────────────────────────────────────
  Maps a numeric resolution_hours value to a human-readable Chinese bucket
  label used for SLA reporting in gold-layer aggregations.

  Bucket thresholds (inclusive upper bound):
    < 0      → '未結案'   (not yet closed / negative means open)
    ≤ 24     → '24hr內'
    ≤ 48     → '24~48hr'
    ≤ 72     → '48~72hr'
    ≤ 120    → '3~5天'
    ≤ 168    → '5~7天'
    ≤ 240    → '7~10天'
    ≤ 360    → '10~15天'
    > 360    → '15天以上'

  NULL is treated as '未結案' (ticket still open, resolution time unknown).

  hours_col must be a SQL expression resolving to a numeric type.
#}
{% macro resolution_bucket(hours_col) %}
  CASE
    WHEN {{ hours_col }} IS NULL OR {{ hours_col }} < 0 THEN '未結案'
    WHEN {{ hours_col }} <= 24                          THEN '24hr內'
    WHEN {{ hours_col }} <= 48                          THEN '24~48hr'
    WHEN {{ hours_col }} <= 72                          THEN '48~72hr'
    WHEN {{ hours_col }} <= 120                         THEN '3~5天'
    WHEN {{ hours_col }} <= 168                         THEN '5~7天'
    WHEN {{ hours_col }} <= 240                         THEN '7~10天'
    WHEN {{ hours_col }} <= 360                         THEN '10~15天'
    ELSE                                                     '15天以上'
  END
{% endmacro %}
