# 🚦 Traffic Congestion Process

**Real-time traffic data pipeline powering a smart map app with congestion visualization and optimal route prediction.**  
Built on **Apache Spark** for scalable ETL and analytics.

---

## 📘 Overview

`traffic_congestion_process` is a real-time data processing system that collects, cleans, and analyzes live traffic data from multiple sources.  
It enables **real-time congestion visualization**, **trend analysis**, and **optimal route prediction** for intelligent transportation systems.

---

### Pre Processing Setup

- Using structured schema to parse the raw JSON files
- Flatten the nested structure
- Select needed fields
- Consolidating and cleaning data
- Save in parquet format for further processing

### Processing Setup

-- label the description fields with id
-- Analyze the traffic congestion level
-- creating the fact table by replace description with id
-- Save the processed data to storage
-- adding timestamp for streaming data

```bash
# Clone repository
git clone https://github.com/dangtnh7904/traffic_congestion_process
cd traffic_congestion_process

# Install dependencies
pip install -r requirements.txt

