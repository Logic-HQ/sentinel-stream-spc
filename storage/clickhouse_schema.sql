-- 1. Create the system database
CREATE DATABASE IF NOT EXISTS sentinel_db;

-- 2. Clean Metric Stream Table Optimized for Time-Series Analysis
CREATE TABLE IF NOT EXISTS sentinel_db.sensor_metrics (
    timestamp DateTime64(3, 'UTC'),
    device_id LowCardinality(String),
    metric_name LowCardinality(String),
    metric_value Float64,
    moving_avg Float64,
    moving_stddev Float64,
    z_score Float64,
    is_spc_anomaly UInt8
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (metric_name, device_id, timestamp);

-- 3. High-Value Anomaly Event Summary Table for Dashboards
CREATE TABLE IF NOT EXISTS sentinel_db.anomaly_alerts (
    alert_id UUID DEFAULT generateUUIDv4(),
    timestamp DateTime64(3, 'UTC'),
    device_id String,
    metric_name String,
    observed_value Float64,
    ucl Float64,
    lcl Float64,
    anomaly_type Enum('SPC_3SIGMA' = 1, 'Z_SCORE_OUTLIER' = 2, 'ISOLATION_FOREST' = 3)
) ENGINE = MergeTree()
ORDER BY (anomaly_type, timestamp);
