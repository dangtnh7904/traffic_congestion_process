# 🗺️ Traffic Data Pipeline & Smart Map System

This repository contains the **data pipeline and backend foundation** for a smart map application — similar to Google Maps — that provides:
- **Real-time traffic congestion visualization**
- **Road dictionary and metadata management**
- **Optimal route prediction** using current and historical flow data

The system is designed for scalability, using **Apache Spark** to handle large volumes of streaming and historical traffic data.

---

## 📦 Features

### 🧩 Data Pipeline (ETL)
- Extracts traffic data (e.g. from HERE, TomTom, or internal APIs)
- Cleans and normalizes nested JSON structures (location, flow, shape)
- Stores curated data in Parquet for analytics
- Exports compact JSON for serving map APIs

### 🧠 Smart Map Backend
- Real-time congestion layer for map visualization
- Dynamic route optimization engine (based on speed, jam factor, flow history)
- Road dictionary builder — keeps consistent road naming and identifiers across updates

### 🌐 Map Integration
- Designed to integrate with web and mobile apps via REST APIs
- Supports visualization similar to Google Maps traffic layer

---

## 🏗️ Project Structure

