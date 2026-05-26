{{ config(materialized='table') }}

WITH raw_transactions AS (
    -- 1. Extract the raw data from the operational table
    SELECT * FROM public.processed_transactions
),

daily_aggregates AS (
    -- 2. Transform the data into daily business metrics
    SELECT 
        DATE(created_at) AS report_date,
        COUNT(txn_id) AS total_transactions,
        SUM(amount) AS total_volume_kes,
        
        -- Business Logic: Isolate the fraud attempts
        COUNT(CASE WHEN is_fraud = true THEN txn_id END) AS fraud_attempts,
        SUM(CASE WHEN is_fraud = true THEN amount ELSE 0 END) AS amount_saved_by_ml_kes

    FROM raw_transactions
    GROUP BY 1
)

-- 3. Final Output with calculated ratios
SELECT 
    report_date,
    total_transactions,
    total_volume_kes,
    fraud_attempts,
    amount_saved_by_ml_kes,
    ROUND((fraud_attempts::numeric / NULLIF(total_transactions, 0)) * 100, 2) AS fraud_attempt_rate_pct
FROM daily_aggregates
ORDER BY report_date DESC