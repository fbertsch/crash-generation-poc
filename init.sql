CREATE SCHEMA IF NOT EXISTS `frank-sandbox.telemetry_stable`;

CREATE TABLE IF NOT EXISTS
  `frank-sandbox.telemetry_stable.crash`
AS
SELECT
  *
FROM
  `mozdata.telemetry.crash`
WHERE
  DATE(submission_timestamp) = DATE("2022-08-15")
LIMIT 1;
