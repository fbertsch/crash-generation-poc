CREATE SCHEMA IF NOT EXISTS `frank-sandbox.telemetry_stable`;

CREATE OR REPLACE TABLE
  `frank-sandbox.telemetry_stable.symbol_files` (
    debug_file STRING,
    debug_id STRING,
    address INT64,
    func STRING
  );
