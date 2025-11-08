-- =====================================================
-- TRAFFIC MAP DATABASE SETUP SCRIPT
-- =====================================================
-- PostgreSQL Database Schema for Traffic Congestion Analytics
-- Run this script to set up the required database structure

-- Create database (run as superuser)
-- CREATE DATABASE traffic_db;

-- Connect to database
\c traffic_db;

-- Enable PostGIS extension for spatial operations
CREATE EXTENSION IF NOT EXISTS postgis;

-- =====================================================
-- TABLE: traffic_events
-- Stores real-time traffic event data
-- =====================================================
CREATE TABLE IF NOT EXISTS traffic_events (
    id SERIAL PRIMARY KEY,
    street_id VARCHAR(255) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    wkt_shape TEXT,
    congestion_level INTEGER,
    speed FLOAT,
    "jamFactor" FLOAT,
    confidence FLOAT,
    traversability VARCHAR(50),
    "freeFlowSpeed" FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_traffic_street_id ON traffic_events(street_id);
CREATE INDEX IF NOT EXISTS idx_traffic_timestamp ON traffic_events(timestamp);
CREATE INDEX IF NOT EXISTS idx_traffic_street_timestamp ON traffic_events(street_id, timestamp DESC);

-- Spatial index for geometry queries (GIST index)
-- This dramatically improves bounding box queries
CREATE INDEX IF NOT EXISTS idx_traffic_geom 
ON traffic_events 
USING GIST (ST_GeomFromText(wkt_shape, 4326))
WHERE wkt_shape IS NOT NULL;

-- =====================================================
-- TABLE: street_labels
-- Stores street name mapping
-- =====================================================
CREATE TABLE IF NOT EXISTS street_labels (
    street_id VARCHAR(255) PRIMARY KEY,
    street_name VARCHAR(500),
    district VARCHAR(100),
    city VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for street name searches
CREATE INDEX IF NOT EXISTS idx_street_name ON street_labels(street_name);

-- =====================================================
-- SAMPLE DATA (Optional - for testing)
-- =====================================================

-- Insert sample street labels
INSERT INTO street_labels (street_id, street_name, district, city) 
VALUES 
    ('street_001', 'Đại Cồ Việt', 'Hai Bà Trưng', 'Hà Nội'),
    ('street_002', 'Giải Phóng', 'Hai Bà Trưng', 'Hà Nội'),
    ('street_003', 'Láng Hạ', 'Ba Đình', 'Hà Nội')
ON CONFLICT (street_id) DO NOTHING;

