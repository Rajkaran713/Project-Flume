# Project-Flume
An AWS powered full fledged Realtime ELT Data Engineering project that extracts various json datasets from Canadian weather website, a public API (MSC GeoMet API), loads, transforms, forecasts and displays the future water levels in a dashboard for effective flood warning.


# 🌊 Project Flume - Real-Time Weather & Water Monitoring Platform

**End-to-end data engineering pipeline with ML-based flood prediction**

![Python](https://img.shields.io/badge/Python-3.12-blue)
![AWS](https://img.shields.io/badge/AWS-S3%20%7C%20Lambda%20%7C%20EC2-orange)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-red)
![ML](https://img.shields.io/badge/ML-Forecasting-green)

---

## 📋 Overview

Project Flume is a production-grade data engineering platform that ingests, processes, and analyzes real-time environmental data from **2,600+ monitoring stations** across Canada. The system combines weather and hydrometric data to predict water level changes and identify flood risks **6-24 hours in advance** using machine learning.

### Key Features

- 🔄 **Real-time Data Ingestion**: Automated ETL pipeline processing 14GB+ daily from Environment Canada APIs
- ⚡ **Serverless Transformation**: AWS Lambda auto-transforms JSON → Parquet with 10x compression
- 🎯 **ML Forecasting**: Time-series models predict water levels 24 hours ahead with confidence intervals
- 📊 **Interactive Dashboard**: Streamlit-based visualization with 2,600+ stations and real-time alerts
- 🏗️ **Medallion Architecture**: Bronze (raw) → Silver (clean) → Gold (aggregated) data layers

---

## 🏛️ Architecture
```
┌─────────────────────────────────────────────────────────────┐
│ DATA SOURCES (Environment Canada)                           │
│ • SWOB (1,100+ weather stations)                            │
│ • Hydrometric (1,500+ water monitoring stations).           |
| • Climate_Hourly (Daily climate data)                       │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────┐
│ INGESTION LAYER (EC2 + Docker)                              │
│ • Cron-based extraction every 5 minutes                     │
│ • Incremental state management                              │
│ • Partitioned S3 writes (year/month/day)                    │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────┐
│ BRONZE LAYER (S3)                                           │
│ • Raw JSON files                                            │
│ • ~120MB per batch                                          │
└──────────────────┬──────────────────────────────────────────┘
                   │ (S3 Event → Lambda)
                   ▼
┌─────────────────────────────────────────────────────────────┐
│ TRANSFORMATION LAYER (AWS Lambda)                           │
│ • Event-driven processing (<1 sec latency)                  │
│ • JSON → Parquet conversion (10x compression)               │
│ • Derived metrics (feels-like temp, wind chill)             │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────┐
│ SILVER LAYER (S3)                                           │
│ • Clean Parquet files (~5MB per batch)                      │
│ • Columnar storage for analytics                            │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────┐
│ GOLD LAYER (S3)                                             │
│ • Pre-aggregated tables:                                    │
│   - station_latest (current conditions)                     │
│   - hourly_summary (time series)                            │
│   - station_metadata (reference)                            │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────┐
│ ML FORECASTING ENGINE                                       │
│ • Linear regression with time features                      │
│ • 24-hour water level predictions                           │
│ • Confidence intervals (95%)                                │
│ • Flood risk identification                                 │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────┐
│ VISUALIZATION LAYER (Streamlit)                             │
│ • Interactive maps (Plotly)                                 │
│ • Real-time metrics & alerts                                │
│ • Historical + forecast charts                              │
│ • Auto-refresh every 5 minutes                              │
└─────────────────────────────────────────────────────────────┘
```

---

## 🚀 Quick Start

### Prerequisites

- Python 3.12+
- AWS Account with S3, Lambda, EC2 access
- AWS CLI configured

### Installation
```bash
# Clone repository
git clone https://github.com/Rajkaran713/Project-Flume.git
cd Project-Flume

# Install dependencies
pip install -r requirements.txt
```

### Running the Dashboard Locally
```bash
cd flume-etl
streamlit run dashboard.py
```

---

## 📂 Project Structure

```
Project-Flume/
├── producer/
│   ├── producer.py              # EC2 data ingestion script
│   ├── Dockerfile               # Container for EC2 deployment
│   └── requirements.txt         # Producer dependencies
├── flume-etl/
│   ├── lambda_transform.py      # Lambda transformation logic
│   ├── create_gold_layer.py     # Gold layer aggregation
│   ├── forecast_water_levels.py # ML forecasting engine
│   ├── dashboard.py             # Streamlit visualization
│   └── transform_to_silver.py   # Silver layer processor
├── terraform/
│   ├── modules/
│   │   └── s3-data-lake/        # Custom reusable S3 module
│   │       ├── main.tf          # S3 bucket with data lake best practices
│   │       ├── variables.tf     # Module input variables
│   │       └── outputs.tf       # Module outputs
│   ├── main.tf                  # Root module orchestrating resources
│   ├── provider.tf              # AWS provider configuration
│   ├── variables.tf             # Root input variables
│   ├── outputs.tf               # Infrastructure outputs
│   ├── lambda.tf                # Lambda function configuration
│   ├── iam.tf                   # IAM roles and policies
│   ├── ec2.tf                   # EC2 instance (optional)
│   └── README.md                # Terraform documentation
├── images/                      # Dashboard screenshots
│   ├── dashboard-overview.png
│   ├── weather-tab.png
│   └── water-forecast.png
├── requirements.txt             # Main Python dependencies
└── README.md                    # This file
```

---

## 💡 Technical Highlights

### Data Engineering

- **Incremental Processing**: State management prevents duplicate ingestion
- **Partitioning Strategy**: Hive-style partitioning (year/month/day) for efficient queries
- **Compression**: 10x reduction (80MB JSON → 8MB Parquet)
- **Serverless**: Event-driven Lambda processing with auto-scaling

### Machine Learning

- **Model**: Linear regression with temporal features
- **Features**: Hour of day, day of week, time index
- **Forecast Horizon**: 24 hours ahead
- **Confidence Intervals**: 95% prediction intervals using residual variance
- **Performance**: 1,715 stations forecasted in <2 minutes

### Cost Optimization

- **EC2**: t2.micro ($8/month or free tier)
- **S3**: ~$1/month for 50GB
- **Lambda**: ~$2/month for 5,000 invocations
- **Total**: ~$11/month (or $3/month with free tier)

---

## 📊 Dashboard Features

### Weather Tab
- 1,100+ weather stations with real-time temperature, humidity, wind
- Interactive map with color-coded temperature markers
- Station-specific trends and historical data
- Hot/cold weather alerts

### Water Levels Tab
- 1,500+ hydrometric stations monitoring rivers and lakes
- Real-time water level and discharge data
- **ML-based 24-hour forecasts** with confidence intervals
- Flood risk alerts (stations with >0.5m predicted rise)
- Provincial filtering

---

## 🔮 ML Forecasting Capabilities

The forecasting engine analyzes historical water level patterns to predict:

- **6-hour forecast**: Immediate flood risk assessment
- **24-hour forecast**: Advanced warning for resource allocation
- **Confidence intervals**: Quantified prediction uncertainty
- **Change detection**: Identifies stations with significant trends

**Example Output:**
```
✓ Generated forecasts for 1,715 stations
  Rising: 21 stations (water levels increasing)
  Falling: 17 stations (water levels decreasing)
  Stable: 1,677 stations (minimal change)
  High Risk: 3 stations (>0.5m rise = flood watch)
```

---

## 🛠️ Tech Stack

**Cloud & Infrastructure:**
- Terraform(AWS Infra Provision)
- AWS S3 (data lake)
- AWS Lambda (serverless compute)
- AWS EC2 (data ingestion)
- Docker (containerization)
- AWS ECR (Container Registry)
- AWS ECS (Cluster Management)

**Data Processing:**
- Python 3.12
- Pandas (data manipulation)
- PyArrow (Parquet I/O)
- Boto3 (AWS SDK)
- Athena(Adhoc-analysis) 

**Machine Learning:**
- Scikit-learn (regression models)
- NumPy (numerical computing)

**Visualization:**
- Streamlit (dashboard framework)
- Plotly (interactive charts)

---

## 📈 Performance Metrics

- **Ingestion Frequency**: Every 5 minutes
- **End-to-End Latency**: <3 seconds (upload → transform → available)
- **Lambda Execution Time**: 400-2,750ms per batch
- **Forecast Generation**: 1,715 stations in 90 seconds
- **Dashboard Load Time**: <2 seconds
- **Data Retention**: Unlimited (S3 lifecycle policies configurable)

---

## 🔐 Security & Best Practices

- ✅ No hardcoded credentials (IAM roles for EC2/Lambda)
- ✅ Least-privilege access policies
- ✅ Partitioned data for access control
- ✅ No KMS encryption overhead (optimized for cost)
- ✅ .gitignore prevents credential leaks

---

## 🎯 Business Value

**Use Cases:**
- **Emergency Management**: Early flood warnings for municipalities
- **Infrastructure Planning**: Historical water level trends for construction
- **Agriculture**: Irrigation planning based on weather forecasts
- **Insurance**: Risk assessment for flood-prone areas
- **Environmental Monitoring**: Climate change impact analysis

---

## �� Future Enhancements

- [ ] Add precipitation data to improve flood prediction accuracy
- [ ] Implement ARIMA/Prophet models for seasonal trends
- [ ] Deploy dashboard to AWS App Runner for public access
- [ ] Add email/SMS alerts for high-risk stations
- [ ] Historical data backfill (1+ years)
- [ ] Multi-region deployment for redundancy


## 🙏 Acknowledgments

- Data Source: Environment Canada (Open Government License)
- Cloud Provider: AWS
- Visualization Framework: Streamlit

---

**Built with ❤️ for sustainable water management and climate resilience**
