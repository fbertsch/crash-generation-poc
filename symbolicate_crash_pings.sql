CREATE OR REPLACE TABLE
  `frank-sandbox.telemetry_stable.symbolicated_crashes`
AS
WITH crash_frames AS (
  SELECT
    document_id,
    thread_i,
    frame_i,
    frame.ip,
    payload.stack_traces.modules[OFFSET(frame.module_index)].base_addr,
    payload.stack_traces.modules[OFFSET(frame.module_index)].debug_file,
    payload.stack_traces.modules[OFFSET(frame.module_index)].debug_id,
  FROM
    `frank-sandbox.telemetry_stable.crash`
  CROSS JOIN
    UNNEST(payload.stack_traces.threads) AS thread WITH OFFSET AS thread_i
  CROSS JOIN
    UNNEST(thread.frames) AS frame WITH OFFSET AS frame_i
  ORDER BY
    submission_timestamp ASC
), crash_frames_with_offset AS (
  SELECT *,
    CAST(ip AS INT64) - CAST(base_addr AS INT64) AS offset
  FROM
    crash_frames
), symbols AS (
  SELECT
    *
  FROM
    `frank-sandbox.telemetry_stable.symbol_files`
), possible_matches AS (
  SELECT
    document_id,
    thread_i,
    frame_i,
    func,
    ROW_NUMBER() OVER (PARTITION BY document_id, thread_i, frame_i ORDER BY symbols.address DESC) AS rn,
  FROM
    crash_frames_with_offset AS frames
  INNER JOIN
    symbols
    ON symbols.address < frames.offset
       AND symbols.debug_file = frames.debug_file
       AND symbols.debug_id = frames.debug_id
), by_thread AS (
  SELECT
    document_id,
    thread_i,
    ARRAY_AGG(func ORDER BY frame_i) AS thread_stack,
  FROM
    possible_matches
  WHERE
    rn = 1
  GROUP BY
    document_id,
    thread_i
)

SELECT
  document_id,
  ARRAY_AGG(STRUCT(thread_i AS thread_index, thread_stack AS stack) ORDER BY thread_i) AS stacks
FROM
  by_thread
GROUP BY
  document_id
