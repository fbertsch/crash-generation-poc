WITH modules AS (
  SELECT module.debug_file, module.debug_id
  FROM `frank-sandbox.telemetry_stable.crash`
  CROSS JOIN UNNEST(payload.stack_traces.modules) AS module
  ORDER BY submission_timestamp ASC
)

SELECT DISTINCT
  debug_file || '/' || debug_id || '/' || REVERSE(SUBSTR(REVERSE(debug_file), STRPOS(REVERSE(debug_file), '.'))) || 'sym' AS filepath
FROM
  modules
