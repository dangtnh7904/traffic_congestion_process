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

// API: Lấy tất cả traffic events mới nhất
app.get('/api/traffic/latest', async (req, res) => {
  try {
    // Lấy bounding box từ query params (minLon, minLat, maxLon, maxLat)
    const { minLon, minLat, maxLon, maxLat } = req.query;
    
    let query;
    let queryParams = [];
    
    if (minLon && minLat && maxLon && maxLat) {
      // Có bounding box - chỉ lấy dữ liệu trong khu vực
      console.log(`Filtering by bbox: ${minLon},${minLat},${maxLon},${maxLat}`);
      query = `
        WITH latest_events AS (
          SELECT DISTINCT ON (street_id)
            te.street_id,
            sl.street_name,
            te.timestamp,
            ST_AsGeoJSON(ST_GeomFromText(te.wkt_shape, 4326)) as geometry,
            te.congestion_level,
            te.speed,
            te."jamFactor",
            te.confidence,
            te.traversability,
            te."freeFlowSpeed"
          FROM traffic_events te
          LEFT JOIN street_labels sl ON te.street_id = sl.street_id
          WHERE te.wkt_shape IS NOT NULL
            AND ST_Intersects(
              ST_GeomFromText(te.wkt_shape, 4326),
              ST_MakeEnvelope($1, $2, $3, $4, 4326)
            )
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
          SELECT DISTINCT ON (street_id)
            te.street_id,
            sl.street_name,
            te.timestamp,
            ST_AsGeoJSON(ST_GeomFromText(te.wkt_shape, 4326)) as geometry,
            te.congestion_level,
            te.speed,
            te."jamFactor",
            te.confidence,
            te.traversability,
            te."freeFlowSpeed"
          FROM traffic_events te
          LEFT JOIN street_labels sl ON te.street_id = sl.street_id
          WHERE te.wkt_shape IS NOT NULL
          ORDER BY te.street_id, te.timestamp DESC
        )
        SELECT * FROM latest_events
        ORDER BY timestamp DESC
        LIMIT ${DEFAULT_QUERY_LIMIT};
      `;
    }
    
    const result = await pool.query(query, queryParams);
    
    // Chuyển đổi sang GeoJSON format VÀ ĐẢO NGƯỢC TỌA ĐỘ
    const features = result.rows.map(row => {
      const geom = JSON.parse(row.geometry);
      
      // Đảo ngược tọa độ từ [lat, lon] sang [lon, lat]
      if (geom.type === 'LineString' && geom.coordinates) {
        geom.coordinates = geom.coordinates.map(coord => [coord[1], coord[0]]);
      }
      
      return {
        type: 'Feature',
        geometry: geom,
        properties: {
          street_id: row.street_id,
          street_name: row.street_name,
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
        bbox: req.query.minLon ? [req.query.minLon, req.query.minLat, req.query.maxLon, req.query.maxLat] : null
      }
    };

    console.log(`Returned ${features.length} traffic features`);
    res.json(geojson);
  } catch (err) {
    console.error('Lỗi khi lấy dữ liệu:', err);
    res.status(500).json({ error: 'Lỗi server', details: err.message });
  }
});

// API: Lấy traffic events theo khoảng thời gian
app.get('/api/traffic/range', async (req, res) => {
  try {
    const { start, end } = req.query;
    
    const query = `
      SELECT 
        te.street_id,
        sl.street_name,
        te.timestamp,
        ST_AsGeoJSON(ST_GeomFromText(te.wkt_shape, 4326)) as geometry,
        te.congestion_level,
        te.speed,
        te."jamFactor",
        te.confidence,
        te.traversability,
        te."freeFlowSpeed"
      FROM traffic_events te
      LEFT JOIN street_labels sl ON te.street_id = sl.street_id
      WHERE te.wkt_shape IS NOT NULL
        AND te.timestamp >= $1
        AND te.timestamp <= $2
      ORDER BY te.timestamp DESC
      LIMIT ${MAX_QUERY_LIMIT};
    `;
    
    const result = await pool.query(query, [start, end]);
    
    const features = result.rows.map(row => ({
      type: 'Feature',
      geometry: JSON.parse(row.geometry),
      properties: {
        street_id: row.street_id,
        street_name: row.street_name,
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
      features: features
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