
-- =====================================================================================
-- ANALYSIS ON ALREADY-JOINED TABLE: `45399569_dse_prod_scores`
-- -------------------------------------------------------------------------------------
-- This script ONLY reads from your materialized joined table and writes analysis tables.
-- It mirrors the PySpark analysis brief:
--   • Apply per-channel thresholds (DG / FD / CMB).
--   • Summaries by channel (counts, alerts, fraud-in-alert) for DSE vs PROD.
--   • Alert overlap: intersection (both), DSE only, PROD only + percentages.
--   • Fraud overlap within alerts: same three sets + percentages.
--   • Score diagnostics for mismatches: margin distributions vs thresholds.
--   • Hour-of-day distributions for intersections & mismatches, with fraud-in-alerts.
--
-- INPUT TABLE (adjust if needed to fully-qualified project.dataset):
--   `45399569_dse_prod_scores`
--
-- Expected columns (from your schema):
--   lifecycle_id STRING
--   customer_id STRING
--   event_received_at STRING
--   customer_portfolio_channel STRING
--   dse_raw_score FLOAT
--   dse_raw_score_X1000 FLOAT
--   dse_raw_score_abs INTEGER
--   dse_raw_score_X1000_rounded FLOAT
--   dse_mt_score FLOAT
--   dse_mt_score_X1000 FLOAT
--   dse_mt_score_abs INTEGER
--   dse_mt_score_X1000_rounded FLOAT
--   raw_score FLOAT
--   raw_score_X1000 FLOAT
--   raw_score_abs INTEGER
--   raw_score_X1000_rounded FLOAT
--   mt_score FLOAT
--   mt_score_X1000 FLOAT
--   mt_score_abs INTEGER
--   mt_score_X1000_rounded FLOAT
--   FLAG_FRAUD INTEGER
--   FRAUD_TYPE STRING
--
-- OUTPUT TABLES (written to your current dataset unless you prefix them):
--   analysis_rows_alerts
--   summary_by_channel
--   summary_alert_overlap
--   summary_fraud_in_alerts
--   diagnostics_mismatch
--   hourly_distribution
--
-- =====================================================================================

-- 0) Channel thresholds from PySpark (scaled scores x1000)
WITH thresholds AS (
  SELECT 'DG'  AS customer_portfolio_channel, 980 AS th_raw_x1000, 765 AS th_mt_x1000 UNION ALL
  SELECT 'FD'  AS customer_portfolio_channel, 950 AS th_raw_x1000, 735 AS th_mt_x1000 UNION ALL
  SELECT 'CMB' AS customer_portfolio_channel, 935 AS th_raw_x1000, 760 AS th_mt_x1000
),

-- 1) Attach thresholds + compute DSE/PROD alert flags using the same channel thresholds
base AS (
  SELECT
    s.*,
    t.th_raw_x1000,
    t.th_mt_x1000,
    CASE
      WHEN s.dse_raw_score_X1000 >= t.th_raw_x1000
        OR s.dse_mt_score_X1000  >= t.th_mt_x1000
      THEN 1 ELSE 0
    END AS DSE_ALERT,
    CASE
      WHEN s.raw_score_X1000 >= t.th_raw_x1000
        OR s.mt_score_X1000  >= t.th_mt_x1000
      THEN 1 ELSE 0
    END AS PROD_ALERT
  FROM `45399569_dse_prod_scores` s
  LEFT JOIN thresholds t
    ON s.customer_portfolio_channel = t.customer_portfolio_channel
),

-- 2) Label overlap sets for alerting
labeled AS (
  SELECT
    *,
    CASE WHEN DSE_ALERT=1 AND PROD_ALERT=1 THEN 1 ELSE 0 END AS BOTH_ALERT,
    CASE WHEN DSE_ALERT=1 AND PROD_ALERT=0 THEN 1 ELSE 0 END AS DSE_ONLY_ALERT,
    CASE WHEN DSE_ALERT=0 AND PROD_ALERT=1 THEN 1 ELSE 0 END AS PROD_ONLY_ALERT
  FROM base
),

-- 3) Row-level enrichment: compute margins vs thresholds and extract hour-of-day
rows_enriched AS (
  SELECT
    lifecycle_id,
    customer_id,
    customer_portfolio_channel,
    event_received_at,
    SAFE.TIMESTAMP(event_received_at) AS evt_ts_utc,
    EXTRACT(HOUR FROM DATETIME(SAFE.TIMESTAMP(event_received_at), 'Europe/London')) AS hour_of_day_local,

    -- Scores
    dse_raw_score_X1000, dse_mt_score_X1000,
    raw_score_X1000,     mt_score_X1000,

    -- Channel thresholds
    th_raw_x1000, th_mt_x1000,

    -- Alert flags and overlap sets
    DSE_ALERT, PROD_ALERT,
    BOTH_ALERT, DSE_ONLY_ALERT, PROD_ONLY_ALERT,

    -- Fraud markers
    FLAG_FRAUD, FRAUD_TYPE,

    -- Margins to thresholds (positive = above threshold)
    (dse_raw_score_X1000 - th_raw_x1000) AS dse_raw_margin,
    (dse_mt_score_X1000  - th_mt_x1000)  AS dse_mt_margin,
    (raw_score_X1000 - th_raw_x1000)     AS prod_raw_margin,
    (mt_score_X1000  - th_mt_x1000)      AS prod_mt_margin
  FROM labeled
)

-- Persist the row-level analysis table
CREATE OR REPLACE TABLE `analysis_rows_alerts` AS
SELECT * FROM rows_enriched
;

-- 4) Per-channel summary: total counts, alerts, fraud-in-alert for DSE & PROD
CREATE OR REPLACE TABLE `summary_by_channel` AS
SELECT
  customer_portfolio_channel,
  COUNT(*) AS rows_total,
  SUM(DSE_ALERT) AS dse_alert_count,
  SUM(CASE WHEN DSE_ALERT=1 AND FLAG_FRAUD=1 THEN 1 ELSE 0 END) AS dse_fraud_in_alert,
  SUM(PROD_ALERT) AS prod_alert_count,
  SUM(CASE WHEN PROD_ALERT=1 AND FLAG_FRAUD=1 THEN 1 ELSE 0 END) AS prod_fraud_in_alert
FROM `analysis_rows_alerts`
GROUP BY customer_portfolio_channel
;

-- 5) Alert overlap (intersection + exclusives) with coverage percentages
CREATE OR REPLACE TABLE `summary_alert_overlap` AS
WITH base AS (
  SELECT customer_portfolio_channel, BOTH_ALERT, DSE_ONLY_ALERT, PROD_ONLY_ALERT
  FROM `analysis_rows_alerts`
),
by_channel AS (
  SELECT
    customer_portfolio_channel,
    COUNT(*) AS n,
    SUM(BOTH_ALERT)      AS both_alerts,
    SUM(DSE_ONLY_ALERT)  AS dse_only,
    SUM(PROD_ONLY_ALERT) AS prod_only,
    SAFE_DIVIDE(SUM(BOTH_ALERT),      COUNT(*)) AS pct_both_over_all,
    SAFE_DIVIDE(SUM(DSE_ONLY_ALERT),  COUNT(*)) AS pct_dse_only_over_all,
    SAFE_DIVIDE(SUM(PROD_ONLY_ALERT), COUNT(*)) AS pct_prod_only_over_all
  FROM base
  GROUP BY customer_portfolio_channel
),
overall AS (
  SELECT
    '__OVERALL__' AS customer_portfolio_channel,
    COUNT(*) AS n,
    SUM(BOTH_ALERT)      AS both_alerts,
    SUM(DSE_ONLY_ALERT)  AS dse_only,
    SUM(PROD_ONLY_ALERT) AS prod_only,
    SAFE_DIVIDE(SUM(BOTH_ALERT),      COUNT(*)) AS pct_both_over_all,
    SAFE_DIVIDE(SUM(DSE_ONLY_ALERT),  COUNT(*)) AS pct_dse_only_over_all,
    SAFE_DIVIDE(SUM(PROD_ONLY_ALERT), COUNT(*)) AS pct_prod_only_over_all
  FROM base
)
SELECT * FROM overall
UNION ALL
SELECT * FROM by_channel
;

-- 6) Fraud overlap within alerts (intersection + exclusives) and percentages
CREATE OR REPLACE TABLE `summary_fraud_in_alerts` AS
WITH alerted AS (
  SELECT customer_portfolio_channel, BOTH_ALERT, DSE_ONLY_ALERT, PROD_ONLY_ALERT, FLAG_FRAUD
  FROM `analysis_rows_alerts`
  WHERE DSE_ALERT=1 OR PROD_ALERT=1
),
by_channel AS (
  SELECT
    customer_portfolio_channel,
    COUNT(*) AS alerted_rows,
    SUM(CASE WHEN FLAG_FRAUD=1 AND BOTH_ALERT=1      THEN 1 ELSE 0 END) AS fraud_both,
    SUM(CASE WHEN FLAG_FRAUD=1 AND DSE_ONLY_ALERT=1  THEN 1 ELSE 0 END) AS fraud_dse_only,
    SUM(CASE WHEN FLAG_FRAUD=1 AND PROD_ONLY_ALERT=1 THEN 1 ELSE 0 END) AS fraud_prod_only,
    SAFE_DIVIDE(SUM(CASE WHEN FLAG_FRAUD=1 AND BOTH_ALERT=1 THEN 1 ELSE 0 END), NULLIF(COUNT(*),0)) AS pct_fraud_both_over_alerted,
    SAFE_DIVIDE(SUM(CASE WHEN FLAG_FRAUD=1 AND DSE_ONLY_ALERT=1 THEN 1 ELSE 0 END), NULLIF(COUNT(*),0)) AS pct_fraud_dse_only_over_alerted,
    SAFE_DIVIDE(SUM(CASE WHEN FLAG_FRAUD=1 AND PROD_ONLY_ALERT=1 THEN 1 ELSE 0 END), NULLIF(COUNT(*),0)) AS pct_fraud_prod_only_over_alerted
  FROM alerted
  GROUP BY customer_portfolio_channel
),
overall AS (
  SELECT
    '__OVERALL__' AS customer_portfolio_channel,
    COUNT(*) AS alerted_rows,
    SUM(CASE WHEN FLAG_FRAUD=1 AND BOTH_ALERT=1      THEN 1 ELSE 0 END) AS fraud_both,
    SUM(CASE WHEN FLAG_FRAUD=1 AND DSE_ONLY_ALERT=1  THEN 1 ELSE 0 END) AS fraud_dse_only,
    SUM(CASE WHEN FLAG_FRAUD=1 AND PROD_ONLY_ALERT=1 THEN 1 ELSE 0 END) AS fraud_prod_only,
    SAFE_DIVIDE(SUM(CASE WHEN FLAG_FRAUD=1 AND BOTH_ALERT=1 THEN 1 ELSE 0 END), NULLIF(COUNT(*),0)) AS pct_fraud_both_over_alerted,
    SAFE_DIVIDE(SUM(CASE WHEN FLAG_FRAUD=1 AND DSE_ONLY_ALERT=1 THEN 1 ELSE 0 END), NULLIF(COUNT(*),0)) AS pct_fraud_dse_only_over_alerted,
    SAFE_DIVIDE(SUM(CASE WHEN FLAG_FRAUD=1 AND PROD_ONLY_ALERT=1 THEN 1 ELSE 0 END), NULLIF(COUNT(*),0)) AS pct_fraud_prod_only_over_alerted
  FROM alerted
)
SELECT * FROM overall
UNION ALL
SELECT * FROM by_channel
;

-- 7) Score diagnostics for mismatches via margins & quantiles
CREATE OR REPLACE TABLE `diagnostics_mismatch` AS
WITH m AS (
  SELECT
    customer_portfolio_channel,
    CASE WHEN DSE_ONLY_ALERT=1 THEN GREATEST(dse_raw_margin, dse_mt_margin) ELSE NULL END AS dse_only_max_margin_above,
    CASE WHEN DSE_ONLY_ALERT=1 THEN GREATEST(th_raw_x1000 - raw_score_X1000, th_mt_x1000 - mt_score_X1000) ELSE NULL END AS dse_only_prod_deficit,
    CASE WHEN PROD_ONLY_ALERT=1 THEN GREATEST(prod_raw_margin, prod_mt_margin) ELSE NULL END AS prod_only_max_margin_above,
    CASE WHEN PROD_ONLY_ALERT=1 THEN GREATEST(th_raw_x1000 - dse_raw_score_X1000, th_mt_x1000 - dse_mt_score_X1000) ELSE NULL END AS prod_only_dse_deficit
  FROM `analysis_rows_alerts`
),
agg AS (
  SELECT
    customer_portfolio_channel,
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
  GROUP BY customer_portfolio_channel
)
SELECT * FROM agg
;

-- 8) Hour-of-day distribution for overlap sets + fraud-in-alert
CREATE OR REPLACE TABLE `hourly_distribution` AS
WITH b AS (
  SELECT
    customer_portfolio_channel,
    hour_of_day_local,
    BOTH_ALERT, DSE_ONLY_ALERT, PROD_ONLY_ALERT,
    DSE_ALERT, PROD_ALERT,
    FLAG_FRAUD
  FROM `analysis_rows_alerts`
),
rolled AS (
  SELECT
    customer_portfolio_channel,
    hour_of_day_local,
    COUNT(*) AS rows_total,
    SUM(BOTH_ALERT)      AS both_alert_rows,
    SUM(DSE_ONLY_ALERT)  AS dse_only_rows,
    SUM(PROD_ONLY_ALERT) AS prod_only_rows,
    SUM(CASE WHEN BOTH_ALERT=1      AND FLAG_FRAUD=1 THEN 1 ELSE 0 END) AS fraud_in_both,
    SUM(CASE WHEN DSE_ONLY_ALERT=1  AND FLAG_FRAUD=1 THEN 1 ELSE 0 END) AS fraud_in_dse_only,
    SUM(CASE WHEN PROD_ONLY_ALERT=1 AND FLAG_FRAUD=1 THEN 1 ELSE 0 END) AS fraud_in_prod_only,
    SUM(DSE_ALERT) AS dse_alerts_any,
    SUM(PROD_ALERT) AS prod_alerts_any
  FROM b
  GROUP BY customer_portfolio_channel, hour_of_day_local
)
SELECT * FROM rolled
ORDER BY customer_portfolio_channel, hour_of_day_local
;

-- End of script.
