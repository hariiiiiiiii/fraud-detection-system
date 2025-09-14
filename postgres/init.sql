-- Initialize IEEE CIS Fraud Detection Database
CREATE DATABASE IF NOT EXISTS ieee_fraud_db;

\c ieee_fraud_db;

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_transactions_dt ON transactions(TransactionDT);
CREATE INDEX IF NOT EXISTS idx_transactions_amt ON transactions(TransactionAmt);
CREATE INDEX IF NOT EXISTS idx_predictions_fraud ON predictions(is_fraud_predicted);
CREATE INDEX IF NOT EXISTS idx_predictions_time ON predictions(prediction_time);

-- Create a view for real-time fraud metrics
CREATE OR REPLACE VIEW fraud_metrics_view AS
SELECT 
    DATE(prediction_time) as date,
    EXTRACT(hour FROM prediction_time) as hour,
    COUNT(*) as total_transactions,
    SUM(CASE WHEN is_fraud_predicted THEN 1 ELSE 0 END) as fraud_count,
    AVG(fraud_probability) as avg_fraud_score,
    AVG(processing_time_ms) as avg_processing_time
FROM predictions 
WHERE prediction_time >= NOW() - INTERVAL '24 hours'
GROUP BY DATE(prediction_time), EXTRACT(hour FROM prediction_time)
ORDER BY date, hour;