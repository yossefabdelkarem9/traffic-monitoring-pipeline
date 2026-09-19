
-- PostgreSQL Initialization


CREATE DATABASE traffic_dwh;

\connect traffic_dwh;


-- BATCH / HISTORICAL LAYER


CREATE TABLE IF NOT EXISTS staging_traffic_summary (
    batch_id VARCHAR(255) NOT NULL,

    camera_id VARCHAR(50) NOT NULL,

    location VARCHAR(150) NOT NULL,

    latitude DOUBLE PRECISION,

    longitude DOUBLE PRECISION,

    total_vehicles INTEGER NOT NULL,

    avg_speed DOUBLE PRECISION NOT NULL,

    speeding_violations INTEGER NOT NULL,

    congestion_level VARCHAR(20) NOT NULL,

    window_start TIMESTAMP,

    window_end TIMESTAMP
);


CREATE TABLE IF NOT EXISTS hourly_traffic_summary (
    summary_id BIGSERIAL PRIMARY KEY,

    batch_id VARCHAR(255) NOT NULL,

    camera_id VARCHAR(50) NOT NULL,

    location VARCHAR(150) NOT NULL,

    latitude DOUBLE PRECISION,

    longitude DOUBLE PRECISION,

    total_vehicles INTEGER NOT NULL,

    avg_speed DOUBLE PRECISION NOT NULL,

    speeding_violations INTEGER NOT NULL,

    congestion_level VARCHAR(20) NOT NULL,

    window_start TIMESTAMP,

    window_end TIMESTAMP,

    processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_hourly_batch_camera
        UNIQUE (batch_id, camera_id, location)
);


CREATE INDEX IF NOT EXISTS idx_hourly_processed_at
    ON hourly_traffic_summary(processed_at DESC);


CREATE INDEX IF NOT EXISTS idx_hourly_camera
    ON hourly_traffic_summary(camera_id);


-- REAL-TIME EVENT LAYER

CREATE TABLE IF NOT EXISTS realtime_traffic_events (

    event_id VARCHAR(100) PRIMARY KEY,

    event_timestamp TIMESTAMPTZ NOT NULL,

    camera_id VARCHAR(50) NOT NULL,

    location VARCHAR(150) NOT NULL,

    latitude DOUBLE PRECISION,

    longitude DOUBLE PRECISION,

    vehicle_type VARCHAR(50),

    speed_kmh DOUBLE PRECISION NOT NULL,

    is_speeding BOOLEAN NOT NULL,

    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);


CREATE INDEX IF NOT EXISTS idx_realtime_events_timestamp
    ON realtime_traffic_events(event_timestamp DESC);


CREATE INDEX IF NOT EXISTS idx_realtime_events_camera
    ON realtime_traffic_events(camera_id);


-- REAL-TIME METRICS

CREATE TABLE IF NOT EXISTS realtime_traffic_metrics (

    metric_id BIGSERIAL PRIMARY KEY,

    camera_id VARCHAR(50) NOT NULL,

    location VARCHAR(150) NOT NULL,

    window_seconds INTEGER NOT NULL,

    window_end TIMESTAMPTZ NOT NULL,

    vehicles_in_window INTEGER NOT NULL,

    avg_speed DOUBLE PRECISION NOT NULL,

    speeding_violations INTEGER NOT NULL,

    congestion_level VARCHAR(20) NOT NULL,

    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_realtime_camera
        UNIQUE (camera_id, location)
);


CREATE INDEX IF NOT EXISTS idx_realtime_metrics_updated_at
    ON realtime_traffic_metrics(updated_at DESC);


-- DASHBOARD VIEWS

CREATE OR REPLACE VIEW realtime_dashboard_metrics AS

SELECT
    camera_id,
    location,
    window_seconds,
    window_end,
    vehicles_in_window,
    avg_speed,
    speeding_violations,
    congestion_level,
    updated_at

FROM realtime_traffic_metrics;


CREATE OR REPLACE VIEW latest_batch_traffic AS

SELECT
    h.*

FROM hourly_traffic_summary h

JOIN (
    SELECT
        MAX(processed_at) AS max_processed_at

    FROM hourly_traffic_summary
) latest

ON h.processed_at = latest.max_processed_at;