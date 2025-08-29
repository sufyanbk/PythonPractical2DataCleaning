
-- ========================================================================
-- BigQuery ANALYSIS TABLES for `45399569_dse_prod_scores`
-- Scope (PORTFOLIO-BASED THRESHOLDS ONLY):
--   • Restrict analysis to Portfolio IN ('HSBC DIGITAL','FD DIGITAL','CMB DIGITAL').
--   • Apply thresholds by Portfolio (not by customer_portfolio_channel):
--       - HSBC DIGITAL : RAW=980, MT=765
--       - FD DIGITAL   : RAW=950, MT=735
--       - CMB DIGITAL  : RAW=935, MT=760
--   • Build row-level analysis and produce summaries/overlaps/diagnostics/hourly.
-- Notes:
--   • Creates TABLES (materialized). Standard SQL, each statement ends with ';'.
--   • Uses SAFE_CAST for timestamps and guards divisions with NULLIF.
-- ========================================================================

-- ----------------------------------------------------------------------------
-- 0) TEMP FILTERED SOURCE: only the three digital portfolios (case-insensitive)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE _tmp_src_portfolios AS
SELECT
  s.*,
  UPPER(Portfolio) AS portfolio_key
FROM `45399569_dse_prod_scores` AS s
WHERE UPPER(Portfolio) IN ('HSBC DIGITAL','FD DIGITAL','CMB DIGITAL')
;

-- ====================================================================
-- 1) ROW-LEVEL ANALYSIS TABLE (portfolio thresholds)
-- ====================================================================
CREATE OR REPLACE TABLE analysis_rows_alerts AS
WITH
  -- Portfolio thresholds (scaled x1000) keyed by portfolio_key
  thresholds AS (
    SELECT 'HSBC DIGITAL' AS portfolio_key, 980 AS th_raw_x1000, 765 AS th_mt_x1000 UNION ALL
    SELECT 'FD DIGITAL'   AS portfolio_key, 950 AS th_raw_x1000, 735 AS th_mt_x1000 UNION ALL
    SELECT 'CMB DIGITAL'  AS portfolio_key, 935 AS th_raw_x1000, 760 AS th_mt_x1000
  ),
  base AS (
    SELECT
      s.*,
      t.th_raw_x1000,
      t.th_mt_x1000,
      -- DSE alert (portfolio-specific thresholds)
      CASE
        WHEN s.dse_raw_score_X1000 >= t.th_raw_x1000
          OR s.dse_mt_score_X1000  >= t.th_mt_x1000
        THEN 1 ELSE 0
      END AS DSE_ALERT,
      -- PROD alert (portfolio-specific thresholds)
      CASE
        WHEN s.raw_score_X1000 >= t.th_raw_x1000
          OR s.mt_score_X1000  >= t.th_mt_x1000
        THEN 1 ELSE 0
      END AS PROD_ALERT
    FROM _tmp_src_portfolios AS s
    LEFT JOIN thresholds AS t
      USING (portfolio_key)
  ),
  labeled AS (
    SELECT
      *,
      CASE WHEN DSE_ALERT=1 AND PROD_ALERT=1 THEN 1 ELSE 0 END AS BOTH_ALERT,
      CASE WHEN DSE_ALERT=1 AND PROD_ALERT=0 THEN 1 ELSE 0 END AS DSE_ONLY_ALERT,
      CASE WHEN DSE_ALERT=0 AND PROD_ALERT=1 THEN 1 ELSE 0 END AS PROD_ONLY_ALERT
    FROM base
  )
SELECT
  -- identifiers
  lifecycle_id,
  customer_id,
  customer_portfolio_channel,  -- keep for reference, but thresholds are by Portfolio
  Portfolio,
  portfolio_key,               -- normalized portfolio for grouping

  -- event time (original + parsed + local hour)
  event_received_at,
  SAFE_CAST(event_received_at AS TIMESTAMP) AS evt_ts_utc,
  EXTRACT(HOUR FROM DATETIME(SAFE_CAST(event_received_at AS TIMESTAMP), 'Europe/London')) AS hour_of_day_local,

  -- optional business fields
  tbt_tran_amt,
  decision,

  -- scaled scores (x1000)
  dse_raw_score_X1000, dse_mt_score_X1000,
  raw_score_X1000,     mt_score_X1000,

  -- optional rounded matches if present
  CASE WHEN dse_raw_score_X1000_rounded = raw_score_X1000_rounded THEN 1 ELSE 0 END AS shadow_match_rounded,
  CASE WHEN dse_mt_score_X1000_rounded  = mt_score_X1000_rounded  THEN 1 ELSE 0 END AS mt_match_rounded,

  -- thresholds
  th_raw_x1000, th_mt_x1000,

  -- alert flags and overlap sets
  DSE_ALERT, PROD_ALERT,
  BOTH_ALERT, DSE_ONLY_ALERT, PROD_ONLY_ALERT,

  -- fraud markers
  FLAG_FRAUD, FRAUD_TYPE, ACTUAL_FRAUD_REASON,

  -- margins (positive => above threshold)
  (dse_raw_score_X1000 - th_raw_x1000) AS dse_raw_margin,
  (dse_mt_score_X1000  - th_mt_x1000)  AS dse_mt_margin,
  (raw_score_X1000     - th_raw_x1000) AS prod_raw_margin,
  (mt_score_X1000      - th_mt_x1000)  AS prod_mt_margin
FROM labeled
;

-- ====================================================================
-- 2) SUMMARY BY PORTFOLIO (counts, alerts, fraud-in-alert for DSE/PROD)
-- ====================================================================
CREATE OR REPLACE TABLE summary_by_portfolio AS
SELECT
  portfolio_key AS portfolio,
  COUNT(*) AS rows_total,
  -- DSE
  SUM(DSE_ALERT) AS dse_alert_count,
  SUM(CASE WHEN DSE_ALERT=1 AND FLAG_FRAUD=1 THEN 1 ELSE 0 END) AS dse_fraud_in_alert,
  -- PROD
  SUM(PROD_ALERT) AS prod_alert_count,
  SUM(CASE WHEN PROD_ALERT=1 AND FLAG_FRAUD=1 THEN 1 ELSE 0 END) AS prod_fraud_in_alert
FROM analysis_rows_alerts
GROUP BY portfolio_key
;

-- ====================================================================
-- 3) ALERT OVERLAP (intersection / DSE-only / PROD-only) + percentages
-- ====================================================================
CREATE OR REPLACE TABLE summary_alert_overlap AS
WITH b AS (
  SELECT portfolio_key, BOTH_ALERT, DSE_ONLY_ALERT, PROD_ONLY_ALERT
  FROM analysis_rows_alerts
),
by_portfolio AS (
  SELECT
    portfolio_key AS portfolio,
    COUNT(*) AS n,
    SUM(BOTH_ALERT)      AS both_alerts,
    SUM(DSE_ONLY_ALERT)  AS dse_only,
    SUM(PROD_ONLY_ALERT) AS prod_only,
    SAFE_DIVIDE(SUM(BOTH_ALERT),      COUNT(*)) AS pct_both_over_all,
    SAFE_DIVIDE(SUM(DSE_ONLY_ALERT),  COUNT(*)) AS pct_dse_only_over_all,
    SAFE_DIVIDE(SUM(PROD_ONLY_ALERT), COUNT(*)) AS pct_prod_only_over_all
  FROM b
  GROUP BY portfolio_key
),
overall AS (
  SELECT
    '__OVERALL__' AS portfolio,
    COUNT(*) AS n,
    SUM(BOTH_ALERT)      AS both_alerts,
    SUM(DSE_ONLY_ALERT)  AS dse_only,
    SUM(PROD_ONLY_ALERT) AS prod_only,
    SAFE_DIVIDE(SUM(BOTH_ALERT),      COUNT(*)) AS pct_both_over_all,
    SAFE_DIVIDE(SUM(DSE_ONLY_ALERT),  COUNT(*)) AS pct_dse_only_over_all,
    SAFE_DIVIDE(SUM(PROD_ONLY_ALERT), COUNT(*)) AS pct_prod_only_over_all
  FROM b
)
SELECT * FROM overall
UNION ALL
SELECT * FROM by_portfolio
;

-- ====================================================================
-- 4) FRAUD OVERLAP WITHIN ALERTS (intersection/exclusives) + percentages
-- ====================================================================
CREATE OR REPLACE TABLE summary_fraud_in_alerts AS
WITH alerted AS (
  SELECT portfolio_key, BOTH_ALERT, DSE_ONLY_ALERT, PROD_ONLY_ALERT, FLAG_FRAUD
  FROM analysis_rows_alerts
  WHERE DSE_ALERT=1 OR PROD_ALERT=1
),
by_portfolio AS (
  SELECT
    portfolio_key AS portfolio,
    COUNT(*) AS alerted_rows,
    SUM(CASE WHEN FLAG_FRAUD=1 AND BOTH_ALERT=1      THEN 1 ELSE 0 END) AS fraud_both,
    SUM(CASE WHEN FLAG_FRAUD=1 AND DSE_ONLY_ALERT=1  THEN 1 ELSE 0 END) AS fraud_dse_only,
    SUM(CASE WHEN FLAG_FRAUD=1 AND PROD_ONLY_ALERT=1 THEN 1 ELSE 0 END) AS fraud_prod_only,
    SAFE_DIVIDE(SUM(CASE WHEN FLAG_FRAUD=1 AND BOTH_ALERT=1     THEN 1 ELSE 0 END), NULLIF(COUNT(*),0)) AS pct_fraud_both_over_alerted,
    SAFE_DIVIDE(SUM(CASE WHEN FLAG_FRAUD=1 AND DSE_ONLY_ALERT=1 THEN 1 ELSE 0 END), NULLIF(COUNT(*),0)) AS pct_fraud_dse_only_over_alerted,
    SAFE_DIVIDE(SUM(CASE WHEN FLAG_FRAUD=1 AND PROD_ONLY_ALERT=1 THEN 1 ELSE 0 END), NULLIF(COUNT(*),0)) AS pct_fraud_prod_only_over_alerted
  FROM alerted
  GROUP BY portfolio_key
),
overall AS (
  SELECT
    '__OVERALL__' AS portfolio,
    COUNT(*) AS alerted_rows,
    SUM(CASE WHEN FLAG_FRAUD=1 AND BOTH_ALERT=1      THEN 1 ELSE 0 END) AS fraud_both,
    SUM(CASE WHEN FLAG_FRAUD=1 AND DSE_ONLY_ALERT=1  THEN 1 ELSE 0 END) AS fraud_dse_only,
    SUM(CASE WHEN FLAG_FRAUD=1 AND PROD_ONLY_ALERT=1 THEN 1 ELSE 0 END) AS fraud_prod_only,
    SAFE_DIVIDE(SUM(CASE WHEN FLAG_FRAUD=1 AND BOTH_ALERT=1     THEN 1 ELSE 0 END), NULLIF(COUNT(*),0)) AS pct_fraud_both_over_alerted,
    SAFE_DIVIDE(SUM(CASE WHEN FLAG_FRAUD=1 AND DSE_ONLY_ALERT=1 THEN 1 ELSE 0 END), NULLIF(COUNT(*),0)) AS pct_fraud_dse_only_over_alerted,
    SAFE_DIVIDE(SUM(CASE WHEN FLAG_FRAUD=1 AND PROD_ONLY_ALERT=1 THEN 1 ELSE 0 END), NULLIF(COUNT(*),0)) AS pct_fraud_prod_only_over_alerted
  FROM alerted
)
SELECT * FROM overall
UNION ALL
SELECT * FROM by_portfolio
;

-- ====================================================================
-- 5) DIAGNOSTICS FOR MISMATCHES: margins/deficits quantiles
-- ====================================================================
CREATE OR REPLACE TABLE diagnostics_mismatch AS
WITH m AS (
  SELECT
    portfolio_key,
    CASE WHEN DSE_ONLY_ALERT=1 THEN GREATEST(dse_raw_margin, dse_mt_margin) END AS dse_only_max_margin_above,
    CASE WHEN DSE_ONLY_ALERT=1 THEN GREATEST(th_raw_x1000 - raw_score_X1000, th_mt_x1000 - mt_score_X1000) END AS dse_only_prod_deficit,
    CASE WHEN PROD_ONLY_ALERT=1 THEN GREATEST(prod_raw_margin, prod_mt_margin) END AS prod_only_max_margin_above,
    CASE WHEN PROD_ONLY_ALERT=1 THEN GREATEST(th_raw_x1000 - dse_raw_score_X1000, th_mt_x1000 - dse_mt_score_X1000) END AS prod_only_dse_deficit
  FROM analysis_rows_alerts
)
SELECT
  portfolio_key AS portfolio,
  APPROX_QUANTILES(dse_only_max_margin_above, 11)[OFFSET(1)]  AS dse_only_margin_p10,
  APPROX_QUANTILES(dse_only_max_margin_above, 11)[OFFSET(5)]  AS dse_only_margin_p50,
  APPROX_QUANTILES(dse_only_max_margin_above, 11)[OFFSET(9)]  AS dse_only_margin_p90,
  APPROX_QUANTILES(dse_only_prod_deficit, 11)[OFFSET(1)]      AS dse_only_deficit_p10,
  APPROX_QUANTILES(dse_only_prod_deficit, 11)[OFFSET(5)]      AS dse_only_deficit_p50,
  APPROX_QUANTILES(dse_only_prod_deficit, 11)[OFFSET(9)]      AS dse_only_deficit_p90,
  APPROX_QUANTILES(prod_only_max_margin_above, 11)[OFFSET(1)] AS prod_only_margin_p10,
  APPROX_QUANTILES(prod_only_max_margin_above, 11)[OFFSET(5)] AS prod_only_margin_p50,
  APPROX_QUANTILES(prod_only_max_margin_above, 11)[OFFSET(9)] AS prod_only_margin_p90,
  APPROX_QUANTILES(prod_only_dse_deficit, 11)[OFFSET(1)]      AS prod_only_deficit_p10,
  APPROX_QUANTILES(prod_only_dse_deficit, 11)[OFFSET(5)]      AS prod_only_deficit_p50,
  APPROX_QUANTILES(prod_only_dse_deficit, 11)[OFFSET(9)]      AS prod_only_deficit_p90
FROM m
GROUP BY portfolio
;

-- ====================================================================
-- 6) HOUR-OF-DAY DISTRIBUTION (by portfolio & hour, Europe/London)
-- ====================================================================
CREATE OR REPLACE TABLE hourly_distribution AS
WITH b AS (
  SELECT
    portfolio_key,
    hour_of_day_local,
    BOTH_ALERT, DSE_ONLY_ALERT, PROD_ONLY_ALERT,
    DSE_ALERT, PROD_ALERT,
    FLAG_FRAUD
  FROM analysis_rows_alerts
)
SELECT
  portfolio_key AS portfolio,
  hour_of_day_local,
  COUNT(*) AS rows_total,
  SUM(BOTH_ALERT)      AS both_alert_rows,
  SUM(DSE_ONLY_ALERT)  AS dse_only_rows,
  SUM(PROD_ONLY_ALERT) AS prod_only_rows,
  SUM(CASE WHEN BOTH_ALERT=1      AND FLAG_FRAUD=1 THEN 1 ELSE 0 END) AS fraud_in_both,
  SUM(CASE WHEN DSE_ONLY_ALERT=1  AND FLAG_FRAUD=1 THEN 1 ELSE 0 END) AS fraud_in_dse_only,
  SUM(CASE WHEN PROD_ONLY_ALERT=1 AND FLAG_FRAUD=1 THEN 1 ELSE 0 END) AS fraud_in_prod_only,
  SUM(DSE_ALERT)  AS dse_alerts_any,
  SUM(PROD_ALERT) AS prod_alerts_any
FROM b
GROUP BY portfolio, hour_of_day_local
ORDER BY portfolio, hour_of_day_local
;

-- Clean up temp
DROP TABLE _tmp_src_portfolios
;
