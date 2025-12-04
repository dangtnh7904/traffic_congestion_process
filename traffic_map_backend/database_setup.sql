-- Active: 1763946630811@@127.0.0.1@5432@traffic_db
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
CREATE EXTENSION IF NOT EXISTS pgrouting;

CREATE INDEX IF NOT EXISTS idx_road_network_source ON road_network(u);
CREATE INDEX IF NOT EXISTS idx_road_network_target ON road_network(v);
-- =====================================================
-- ALTER STATEMENTS (Run these to update existing schema)
-- =====================================================

-- Drop existing tables to recreate with new schema
DROP TABLE IF EXISTS traffic_events CASCADE;
DROP TABLE IF EXISTS road_segments CASCADE;

DROP TABLE IF EXISTS road_network CASCADE;

CREATE TABLE IF NOT EXISTS road_network (
    -- Composite Primary Key: (u, v) to handle bidirectional roads
    -- Same osmid can appear twice: once for (u→v) and once for (v→u)
    u BIGINT NOT NULL,       -- Start node ID (for graph algorithms)
    v BIGINT NOT NULL,       -- End node ID (for graph algorithms)
    -- OpenStreetMap attributes
    osmid TEXT NOT NULL,   -- OSM Way ID (can be duplicated for bidirectional)
    name TEXT,               -- Street name
    oneway BOOLEAN,          -- One-way road flag
    highway TEXT,            -- Road type: 'motorway', 'primary', 'secondary', etc.
    -- Routing cost attributes
    speed_limit FLOAT,       -- Speed limit in km/h
    lanes TEXT,              -- Number of lanes
    length FLOAT NOT NULL,   -- Road length in meters (for cost calculation)
    azimuth FLOAT,           -- Direction in degrees (0-360)
    -- Geometry (full road LineString)
    geometry GEOMETRY(LineString, 4326),  -- WGS84 coordinate system
    -- Composite primary key for bidirectional routing
    PRIMARY KEY (u, v)
);

-- =====================================================
-- TABLE: road_segments (Child - Split for Map Matching)
-- Stores roads split into small chunks (~100m) for precise map matching
-- Each record is a sub-segment of a parent road from road_network
-- =====================================================
CREATE TABLE IF NOT EXISTS road_segments (
    -- Primary key: Auto-increment INTEGER (FAST for joins and FKs)
    segment_id VARCHAR(64) NOT NULL PRIMARY KEY,    
    -- Foreign key to parent road
    osmid TEXT NOT NULL,   -- OSM Way ID (can be single or array like "[123,456]")    
    -- OpenStreetMap attributes (inherited from parent)
    name TEXT,
    oneway BOOLEAN,
    highway TEXT,    
    -- Topology (inherited from parent)
    u BIGINT,                -- Parent start node
    v BIGINT,                -- Parent end node   
    -- Segment-specific attributes
    speed_limit FLOAT,
    lanes TEXT,
    length FLOAT NOT NULL,   -- Actual segment length (~100m)
    azimuth FLOAT,           -- Segment direction (for direction matching)   
    -- Geometry (small LineString chunk)
    geometry GEOMETRY(LineString, 4326)    
    -- Note: No FK constraint to road_network.osmid because:
    -- 1. osmid is not unique (bidirectional roads have 2 rows with same osmid)
    -- 2. Segments are derived from road_network but don't need referential integrity
    -- 3. Use osmid as informational field for traceability only
);

CREATE TABLE IF NOT EXISTS traffic_events (
    id SERIAL PRIMARY KEY,    
    -- Foreign key to road_network table (segment_id)
    street_id VARCHAR(64) NOT NULL,   
    -- Timestamp of traffic data
    timestamp TIMESTAMP NOT NULL,    
    -- Traffic metrics from HERE API
    speed FLOAT,                    -- Current speed (km/h)
    "jamFactor" FLOAT,              -- Congestion level 0-10 (0=free flow, 10=standstill)
    confidence FLOAT,               -- Data confidence 0-1
    traversability VARCHAR(50),     -- Road status: 'open', 'closed', etc.
    "freeFlowSpeed" FLOAT,          -- Free flow speed (km/h)    
    -- Derived congestion level
    congestion_level VARCHAR(20),   -- 'Low', 'Medium', 'High'    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  
    -- Foreign key constraint to reference road_segments (child table)
    CONSTRAINT fk_street_segment 
        FOREIGN KEY (street_id) 
        REFERENCES road_segments(segment_id) 
        ON DELETE CASCADE
);

-- =====================================================
-- INDEXES for road_network (Parent Table - Routing)
-- =====================================================

-- Spatial index (CRITICAL for geometry queries)
CREATE INDEX IF NOT EXISTS idx_road_network_geom 
    ON road_network 
    USING GIST (geometry);

-- Indexes for graph traversal (Dijkstra, A*)
CREATE INDEX IF NOT EXISTS idx_road_network_u 
    ON road_network(u);

CREATE INDEX IF NOT EXISTS idx_road_network_v 
    ON road_network(v);

-- Index on osmid for lookups (non-unique, can have duplicates)
CREATE INDEX IF NOT EXISTS idx_road_network_osmid 
    ON road_network(osmid);

-- Index on highway for filtering road types
CREATE INDEX IF NOT EXISTS idx_road_network_highway 
    ON road_network(highway);

-- Index on oneway for routing constraints
CREATE INDEX IF NOT EXISTS idx_road_network_oneway 
    ON road_network(oneway);

-- Index on name for street name searches
CREATE INDEX IF NOT EXISTS idx_road_network_name 
    ON road_network(name);

-- =====================================================
-- INDEXES for road_segments (Child Table - Map Matching)
-- =====================================================

-- Spatial index (CRITICAL for map matching - speeds up by 100x)
CREATE INDEX IF NOT EXISTS idx_road_segments_geom 
    ON road_segments 
    USING GIST (geometry);

-- Index on osmid for joining with parent
CREATE INDEX IF NOT EXISTS idx_road_segments_osmid 
    ON road_segments(osmid);

-- Index on highway for zoom-based filtering
CREATE INDEX IF NOT EXISTS idx_road_segments_highway 
    ON road_segments(highway);

-- Index on name for street searches
CREATE INDEX IF NOT EXISTS idx_road_segments_name 
    ON road_segments(name);

-- Index on azimuth for direction matching
CREATE INDEX IF NOT EXISTS idx_road_segments_azimuth 
    ON road_segments(azimuth);

-- Composite index for spatial + highway filtering (common query pattern)
CREATE INDEX IF NOT EXISTS idx_road_segments_highway_geom 
    ON road_segments(highway) 
    INCLUDE (geometry);

-- =====================================================
-- INDEXES for traffic_events (Traffic Data)
-- =====================================================

-- Index on street_id for fast lookups by segment
CREATE INDEX IF NOT EXISTS idx_traffic_street_id 
    ON traffic_events(street_id);

-- Index on timestamp for time-based queries
CREATE INDEX IF NOT EXISTS idx_traffic_timestamp 
    ON traffic_events(timestamp DESC);

-- Composite index for segment + time (most common query pattern)
CREATE INDEX IF NOT EXISTS idx_traffic_street_timestamp 
    ON traffic_events(street_id, timestamp DESC);

-- Index on congestion level for filtering
CREATE INDEX IF NOT EXISTS idx_traffic_congestion 
    ON traffic_events(congestion_level);

-- Index on jamFactor for range queries
CREATE INDEX IF NOT EXISTS idx_traffic_jam_factor 
    ON traffic_events("jamFactor");

ALTER TABLE road_network DROP CONSTRAINT IF EXISTS road_network_pkey;
CREATE INDEX idx_road_network_u_v ON road_network (u, v);

-- Note: Partial index with NOW() removed because NOW() is VOLATILE
-- For recent data queries, use the composite index idx_traffic_street_timestamp instead

-- =====================================================
-- COMMENTS for documentation
-- =====================================================
COMMENT ON TABLE road_network IS 'Parent table: Full road edges for routing (u→v graph)';
COMMENT ON TABLE road_segments IS 'Child table: Roads split into ~100m chunks for map matching';
COMMENT ON TABLE traffic_events IS 'Real-time traffic status mapped to road segments';

COMMENT ON COLUMN road_network.osmid IS 'OSM Way ID - can be array like "[123,456]" when ways are merged';
COMMENT ON COLUMN road_segments.segment_id IS 'Auto-increment INTEGER for fast joins (5-10x faster than TEXT)';
COMMENT ON COLUMN traffic_events.street_id IS 'FK to road_segments.segment_id';


#count overlapping road segments (for data quality check)
SELECT 
    COUNT(*) as tong_so_loi,
    SUM(CASE WHEN kieu_loi = 'Cắt nhau (Crosses)' THEN 1 ELSE 0 END) as sl_cau_vuot,
    SUM(CASE WHEN kieu_loi = 'Đè lên nhau (Overlaps)' THEN 1 ELSE 0 END) as sl_loi_du_lieu
FROM (
    SELECT 
        a.osmid,
        b.osmid,
        CASE 
            WHEN ST_Crosses(a.geometry, b.geometry) THEN 'Cắt nhau (Crosses)'
            WHEN ST_Overlaps(a.geometry, b.geometry) THEN 'Đè lên nhau (Overlaps)'
            ELSE 'Khác' 
        END as kieu_loi
    FROM road_network a
    JOIN road_network b 
        -- Thay vì segment_id, ta dùng so sánh bộ 3 (osmid, u, v) để đảm bảo
        -- mỗi cặp chỉ được so sánh 1 lần và không so sánh với chính nó
        ON (a.osmid, a.u, a.v) < (b.osmid, b.u, b.v)
    WHERE 
        -- 1. Có dính dáng không gian
        ST_Intersects(a.geometry, b.geometry)
        -- 2. Loại bỏ trường hợp chạm đầu/đuôi (kết nối hợp lệ)
        AND NOT ST_Touches(a.geometry, b.geometry)
) sub;

SELECT 
    a.osmid as road_1_id,
    a.name as road_1_name,
    b.osmid as road_2_id,
    b.name as road_2_name,
    CASE 
        WHEN ST_Crosses(a.geometry, b.geometry) THEN 'Cắt nhau (Cầu vượt?)'
        WHEN ST_Overlaps(a.geometry, b.geometry) THEN 'Đè lên nhau (Lỗi!)'
        ELSE 'Khác'
    END as loai_loi,
    -- Lấy phần hình học giao nhau để xem nó nằm ở đâu
    ST_AsText(ST_Intersection(a.geometry, b.geometry)) as vi_tri_loi
FROM road_network a
JOIN road_network b 
    ON (a.osmid, a.u, a.v) < (b.osmid, b.u, b.v)
WHERE 
    ST_Intersects(a.geometry, b.geometry)
    AND NOT ST_Touches(a.geometry, b.geometry)
LIMIT 20;

SELECT 
    COUNT(*) as tong_so_segment,
    COUNT(DISTINCT segment_id) as so_pk_duy_nhat,
    COUNT(*) FILTER (WHERE geometry IS NULL) as so_loi_null_geom,
    COUNT(*) FILTER (WHERE ST_IsValid(geometry) = false) as so_loi_invalid_geom,
    MIN(length) as do_dai_min,
    MAX(length) as do_dai_max, -- Nên xấp xỉ 100m (hoặc chunk_size của bạn)
    AVG(length) as do_dai_tb
FROM road_segments;

SELECT 
    COUNT(*) as tong_so_cap_loi,
    SUM(CASE WHEN kieu_loi = 'Cắt nhau (Crosses)' THEN 1 ELSE 0 END) as sl_cau_vuot,
    SUM(CASE WHEN kieu_loi = 'Đè lên nhau (Overlaps)' THEN 1 ELSE 0 END) as sl_loi_nghiem_trong
FROM (
    SELECT 
        a.segment_id, 
        b.segment_id,
        CASE 
            WHEN ST_Crosses(a.geometry, b.geometry) THEN 'Cắt nhau (Crosses)'
            WHEN ST_Overlaps(a.geometry, b.geometry) THEN 'Đè lên nhau (Overlaps)'
            ELSE 'Khác'
        END as kieu_loi
    FROM road_segments a
    JOIN road_segments b 
        ON a.segment_id < b.segment_id -- Tránh so sánh lặp
    WHERE 
        -- 1. Tìm cặp có dính dáng
        ST_Intersects(a.geometry, b.geometry)
        -- 2. LOẠI BỎ trường hợp chạm đầu/đuôi (Nối đoạn 0 với đoạn 1 là bình thường)
        AND NOT ST_Touches(a.geometry, b.geometry)
        -- 3. (Tùy chọn) Chỉ check trong cùng 1 con đường gốc để xem cắt có bị lỗi không
        -- AND a.osmid = b.osmid 
) sub;

select speed from traffic_events
ORDER BY speed desc;