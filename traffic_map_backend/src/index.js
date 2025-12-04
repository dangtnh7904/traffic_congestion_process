const express = require('express');
const { Pool } = require('pg');
const cors = require('cors');
require('dotenv').config();

const app = express();
const port = process.env.PORT || 3000;

// Enable CORS để Flutter app có thể gọi API
app.use(cors({
  origin: process.env.CORS_ORIGIN || '*'
}));
app.use(express.json());

// Kết nối PostgreSQL với environment variables
const pool = new Pool({
  host: process.env.DB_HOST || 'localhost',
  port: parseInt(process.env.DB_PORT) || 5432,
  database: process.env.DB_NAME || 'traffic_db',
  user: process.env.DB_USER || 'postgres',
  password: process.env.DB_PASSWORD || 'postgres',
  min: parseInt(process.env.DB_POOL_MIN) || 2,
  max: parseInt(process.env.DB_POOL_MAX) || 10,
  idleTimeoutMillis: parseInt(process.env.DB_IDLE_TIMEOUT) || 10000,
  connectionTimeoutMillis: parseInt(process.env.DB_CONNECTION_TIMEOUT) || 3000,
});

// Constants from environment
const MAX_QUERY_LIMIT = parseInt(process.env.MAX_QUERY_LIMIT) || 5000;
const DEFAULT_QUERY_LIMIT = parseInt(process.env.DEFAULT_QUERY_LIMIT) || 200;
const BBOX_QUERY_LIMIT = parseInt(process.env.BBOX_QUERY_LIMIT) || 500;

// Color configuration for congestion levels (overridable via .env)
const COLOR_CONFIG = {
  low: process.env.COLOR_LOW || '#00BCD4',
  medium: process.env.COLOR_MEDIUM || '#FF9800',
  high: process.env.COLOR_HIGH || '#E91E63',
  severe: process.env.COLOR_SEVERE || '#9C27B0',
  default: process.env.COLOR_DEFAULT || '#2196F3'
};

// Test database connection
pool.query('SELECT NOW()', (err, res) => {
  if (err) {
    console.error(' Lỗi kết nối database:', err.message);
    console.error(' Kiểm tra lại thông tin trong file .env');
  } else {
    console.log(' Kết nối database thành công!');
    console.log(` Server time: ${res.rows[0].now}`);
    console.log(` Database: ${process.env.DB_NAME}@${process.env.DB_HOST}:${process.env.DB_PORT}`);
  }
});

// API: Lấy tất cả traffic events mới nhất (JOIN với road_segments)
app.get('/api/traffic/latest', async (req, res) => {
  try {
    // Lấy bounding box và zoom level từ query params
    const { minLon, minLat, maxLon, maxLat, zoom } = req.query;
    const zoomLevel = parseFloat(zoom) || 13;
    
    // Xác định highway types để hiển thị dựa trên zoom level
    let highwayCondition = '';
    if (zoomLevel < 12) {
      // Zoom xa - chỉ hiển thị đường lớn
      highwayCondition = "rs.highway IN ('motorway', 'trunk', 'primary')";
    } else if (zoomLevel < 14) {
      // Zoom trung bình - thêm secondary
      highwayCondition = "rs.highway IN ('motorway', 'trunk', 'trunk_link', 'primary', 'primary_link', 'secondary', 'secondary_link')";
    } else if (zoomLevel < 16) {
      // Zoom gần - thêm tertiary
      highwayCondition = "rs.highway IN ('motorway', 'trunk', 'trunk_link', 'primary', 'primary_link', 'secondary', 'secondary_link', 'tertiary', 'tertiary_link')";
    }
    // Zoom >= 16: hiển thị tất cả (không filter)

    const highwayClauseWithAnd = highwayCondition ? `AND ${highwayCondition}` : '';
    const highwayClauseWithWhere = highwayCondition ? `WHERE ${highwayCondition}` : '';
    
    let query;
    let queryParams = [];
    
    if (minLon && minLat && maxLon && maxLat) {
      // Có bounding box - chỉ lấy dữ liệu trong khu vực
      console.log(`Filtering by bbox: ${minLon},${minLat},${maxLon},${maxLat}, zoom: ${zoomLevel}`);
      query = `
        WITH latest_events AS (
          SELECT DISTINCT ON (te.street_id)
            te.street_id,
            rs.name as street_name,
            rs.highway,
            rs.oneway,
            te.timestamp,
            ST_AsGeoJSON(rs.geometry) as geometry,
            te.congestion_level,
            te.speed,
            te."jamFactor",
            te.confidence,
            te.traversability,
            te."freeFlowSpeed"
          FROM traffic_events te
          INNER JOIN road_segments rs ON te.street_id = rs.segment_id
          WHERE ST_Intersects(
              rs.geometry,
              ST_MakeEnvelope($1, $2, $3, $4, 4326)
            )
            ${highwayClauseWithAnd}
          ORDER BY te.street_id, te.timestamp DESC
        )
        SELECT * FROM latest_events
        ORDER BY timestamp DESC
        LIMIT ${BBOX_QUERY_LIMIT};
      `;
      queryParams = [minLon, minLat, maxLon, maxLat];
    } else {
      // Không có bounding box - trả về tất cả (giới hạn)
      query = `
        WITH latest_events AS (
          SELECT DISTINCT ON (te.street_id)
            te.street_id,
            rs.name as street_name,
            rs.highway,
            rs.oneway,
            te.timestamp,
            ST_AsGeoJSON(rs.geometry) as geometry,
            te.congestion_level,
            te.speed,
            te."jamFactor",
            te.confidence,
            te.traversability,
            te."freeFlowSpeed"
          FROM traffic_events te
          INNER JOIN road_segments rs ON te.street_id = rs.segment_id
          ${highwayClauseWithWhere}
          ORDER BY te.street_id, te.timestamp DESC
        )
        SELECT * FROM latest_events
        ORDER BY timestamp DESC
        LIMIT ${DEFAULT_QUERY_LIMIT};
      `;
    }
    
    const result = await pool.query(query, queryParams);
    
    // Chuyển đổi sang GeoJSON format
    const features = result.rows.map(row => {
      const geom = JSON.parse(row.geometry);
      
      return {
        type: 'Feature',
        geometry: geom,
        properties: {
          street_id: row.street_id,
          street_name: row.street_name,
          highway: row.highway,
          oneway: row.oneway,
          timestamp: row.timestamp,
          congestion_level: row.congestion_level,
          speed: row.speed,
          jamfactor: row.jamFactor,
          confidence: row.confidence,
          traversability: row.traversability,
          freeflowspeed: row.freeFlowSpeed,
        }
      };
    });

    const geojson = {
      type: 'FeatureCollection',
      features: features,
      metadata: {
        count: features.length,
        zoom: zoomLevel,
        bbox: req.query.minLon ? [req.query.minLon, req.query.minLat, req.query.maxLon, req.query.maxLat] : null,
        color_config: COLOR_CONFIG
      }
    };

    console.log(`Returned ${features.length} traffic features (zoom: ${zoomLevel})`);
    res.json(geojson);
  } catch (err) {
    console.error('Lỗi khi lấy dữ liệu:', err);
    res.status(500).json({ error: 'Lỗi server', details: err.message });
  }
});

// API: Lấy traffic events theo khoảng thời gian (hỗ trợ bbox và highway filter)
app.get('/api/traffic/range', async (req, res) => {
  try {
    const { start, end, minLon, minLat, maxLon, maxLat, zoom } = req.query;

    if (!start || !end) {
      return res.status(400).json({ error: 'Thiếu tham số start hoặc end (ISO time)' });
    }

    const zoomLevel = parseFloat(zoom) || 13;
    const requestLimit = parseInt(req.query.limit) || 0;
    const baseLimit = minLon && minLat && maxLon && maxLat ? BBOX_QUERY_LIMIT : DEFAULT_QUERY_LIMIT;
    const limit = Math.min(requestLimit > 0 ? requestLimit : baseLimit, MAX_QUERY_LIMIT);

    let highwayCondition = '';
    if (zoomLevel < 12) {
      highwayCondition = "rs.highway IN ('motorway', 'trunk', 'primary')";
    } else if (zoomLevel < 14) {
      highwayCondition = "rs.highway IN ('motorway', 'trunk', 'trunk_link', 'primary', 'primary_link', 'secondary', 'secondary_link')";
    } else if (zoomLevel < 16) {
      highwayCondition = "rs.highway IN ('motorway', 'trunk', 'trunk_link', 'primary', 'primary_link', 'secondary', 'secondary_link', 'tertiary', 'tertiary_link')";
    }

    const highwayClause = highwayCondition ? `AND ${highwayCondition}` : '';

    let query;
    let queryParams = [];

    if (minLon && minLat && maxLon && maxLat) {
      query = `
        SELECT
          te.street_id,
          rs.name as street_name,
          rs.highway,
          rs.oneway,
          te.timestamp,
          ST_AsGeoJSON(rs.geometry) as geometry,
          te.congestion_level,
          te.speed,
          te."jamFactor",
          te.confidence,
          te.traversability,
          te."freeFlowSpeed"
        FROM traffic_events te
        INNER JOIN road_segments rs ON te.street_id = rs.segment_id
        WHERE te.timestamp BETWEEN $5 AND $6
          AND ST_Intersects(
            rs.geometry,
            ST_MakeEnvelope($1, $2, $3, $4, 4326)
          )
          ${highwayClause}
        ORDER BY te.timestamp DESC
        LIMIT ${limit};
      `;
      queryParams = [minLon, minLat, maxLon, maxLat, start, end];
    } else {
      query = `
        SELECT
          te.street_id,
          rs.name as street_name,
          rs.highway,
          rs.oneway,
          te.timestamp,
          ST_AsGeoJSON(rs.geometry) as geometry,
          te.congestion_level,
          te.speed,
          te."jamFactor",
          te.confidence,
          te.traversability,
          te."freeFlowSpeed"
        FROM traffic_events te
        INNER JOIN road_segments rs ON te.street_id = rs.segment_id
        WHERE te.timestamp BETWEEN $1 AND $2
          ${highwayClause}
        ORDER BY te.timestamp DESC
        LIMIT ${limit};
      `;
      queryParams = [start, end];
    }

    const result = await pool.query(query, queryParams);

    const features = result.rows.map(row => ({
      type: 'Feature',
      geometry: JSON.parse(row.geometry),
      properties: {
        street_id: row.street_id,
        street_name: row.street_name,
        highway: row.highway,
        oneway: row.oneway,
        timestamp: row.timestamp,
        congestion_level: row.congestion_level,
        speed: row.speed,
        jamfactor: row.jamFactor,
        confidence: row.confidence,
        traversability: row.traversability,
        freeflowspeed: row.freeFlowSpeed,
      }
    }));

    const geojson = {
      type: 'FeatureCollection',
      features,
      metadata: {
        count: features.length,
        zoom: zoomLevel,
        bbox: minLon ? [minLon, minLat, maxLon, maxLat] : null,
        time_range: [start, end],
        query_limit: limit,
        color_config: COLOR_CONFIG
      }
    };

    res.json(geojson);
  } catch (err) {
    console.error('Lỗi khi lấy dữ liệu:', err);
    res.status(500).json({ error: 'Lỗi server', details: err.message });
  }
});

// API: Thống kê
app.get('/api/stats', async (req, res) => {
  try {
    const query = `
      SELECT 
        COUNT(*) as total_events,
        COUNT(DISTINCT street_id) as total_streets,
        MAX(timestamp) as latest_update,
        AVG(speed) as avg_speed,
        AVG("jamFactor") as avg_jamfactor
      FROM traffic_events;
    `;
    
    const result = await pool.query(query);
    res.json(result.rows[0]);
  } catch (err) {
    console.error('Lỗi khi lấy thống kê:', err);
    res.status(500).json({ error: 'Lỗi server', details: err.message });
  }
});

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'OK', message: 'Backend đang chạy tốt' });
});

// Root endpoint
app.get('/', (req, res) => {
  res.json({
    message: 'Traffic Map API',
    endpoints: {
      '/api/traffic/latest': 'Lấy dữ liệu giao thông mới nhất',
      '/api/traffic/range?start=<ISO_TIME>&end=<ISO_TIME>': 'Lấy dữ liệu theo khoảng thời gian',
      '/api/stats': 'Thống kê tổng quan',
      '/health': 'Kiểm tra trạng thái server'
    }
  });
});

app.listen(port, () => {
  console.log(' ================================');
  console.log(` Traffic Map Backend API`);
  console.log(` Server: http://${process.env.HOST || 'localhost'}:${port}`);
  console.log(` API docs: http://${process.env.HOST || 'localhost'}:${port}/`);
  console.log(` Environment: ${process.env.NODE_ENV || 'development'}`);
  console.log(' ================================');
});