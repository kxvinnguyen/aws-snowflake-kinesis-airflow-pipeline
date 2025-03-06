-- Create database and schema
CREATE DATABASE IF NOT EXISTS data_pipeline_db;
USE DATABASE data_pipeline_db;
CREATE SCHEMA IF NOT EXISTS public;
USE SCHEMA public;

-- Create raw data landing table
CREATE TABLE IF NOT EXISTS raw_data (
    event_id VARCHAR,
    event_timestamp TIMESTAMP_NTZ,
    event_type VARCHAR,
    user_id VARCHAR,
    data VARIANT,
    inserted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create a view for analytics
CREATE OR REPLACE VIEW user_events AS
SELECT
    event_id,
    event_timestamp,
    event_type,
    user_id,
    data:product_id::VARCHAR as product_id,
    data:price::FLOAT as price,
    inserted_at
FROM raw_data
WHERE event_type = 'purchase';

-- Create a daily summary table
CREATE OR REPLACE TABLE daily_event_summary AS
SELECT 
    DATE(event_timestamp) as event_date,
    event_type,
    COUNT(*) as event_count,
    MIN(event_timestamp) as first_event,
    MAX(event_timestamp) as last_event
FROM raw_data
GROUP BY event_date, event_type; 