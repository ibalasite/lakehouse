{#
  response_bucket(hours_col)
  ─────────────────────────────────────────────────────────────────────────────
  Maps a numeric response_hours value to a human-readable Chinese bucket label
  used for first-response SLA reporting in gold-layer aggregations.

  Bucket thresholds (inclusive upper bound):
    < 0    → '未回覆'   (not yet replied / negative means no response recorded)
    ≤ 4    → '4hr內'
    ≤ 8    → '4~8hr'
    ≤ 12   → '8~12hr'
    ≤ 24   → '12~24hr'
    ≤ 48   → '24~48hr'
    ≤ 72   → '48~72hr'
    > 72   → '72hr以上'

  NULL is treated as '未回覆' (no first-response timestamp recorded).

  hours_col must be a SQL expression resolving to a numeric type.
#}
{% macro response_bucket(hours_col) %}
  CASE
    WHEN {{ hours_col }} IS NULL OR {{ hours_col }} < 0 THEN '未回覆'
    WHEN {{ hours_col }} <= 4                           THEN '4hr內'
    WHEN {{ hours_col }} <= 8                           THEN '4~8hr'
    WHEN {{ hours_col }} <= 12                          THEN '8~12hr'
    WHEN {{ hours_col }} <= 24                          THEN '12~24hr'
    WHEN {{ hours_col }} <= 48                          THEN '24~48hr'
    WHEN {{ hours_col }} <= 72                          THEN '48~72hr'
    ELSE                                                     '72hr以上'
  END
{% endmacro %}
