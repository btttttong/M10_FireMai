# FireMai Wildfire Monitoring Pipeline

## 🔍 Overview
**FireMai** is a fully serverless, end-to-end data engineering pipeline that collects wildfire data from the **GISTDA API**, stores it in **Google Cloud Storage (GCS)**, loads it into **BigQuery** for analysis, enriches it with nearby locations using the **Google Places API**, and visualizes insights in **Looker Studio**.

---

## 📊 Architecture
```
            Cloud Scheduler (Every 8 hours)
                     ↓
          [Cloud Run: load_api_to_gcs] ---------------------------
                     ↓                                     |
       (GISTDA API → Fire JSON in GCS)              Optional Retry
                     ↓                                     |
     GCS Bucket: FireMai/{timestamp}.json                 ↓
                     ↓                             Pub/Sub Topic
          [Cloud Run: gcs_to_bigquery] ▶ Dedup + Upload to BigQuery
                     ↓
              BigQuery Table: fire_data
                     ↓
     [Pub/Sub Trigger] ▶ Trigger analyze_nearby_areas
                     ↓
     [Cloud Run: analyze_nearby_areas]
            └─ Enrich fire records with nearby schools, hospitals, etc.
                     ↓
              BigQuery Table: fire_data_nearby_places
                     ↓
              Looker Studio Dashboard
```

---

## 📦 Components

### 1. `load_api_to_gcs`
- Scheduled via **Cloud Scheduler**
- Pulls JSON fire data from the GISTDA API
- Saves raw files to GCS as structured JSON

### 2. `gcs_to_bigquery`
- Triggered by **GCS Pub/Sub Notification**
- Loads new files from GCS
- Deduplicates using a unique key
- Appends to BigQuery table `fire_data`
- Publishes each recent record to Pub/Sub (`firemai-nearby-trigger`)

### 3. `analyze_nearby_areas`
- Triggered by Pub/Sub
- Fetches nearby places using **Google Places API** for each fire record
- Stores enriched data to BigQuery table `fire_data_nearby_places`

### 4. Dashboard
- Built in **Looker Studio**
- Allows filtering by province, shows hotspots, nearby places, trend over time

---

## ⚖️ Pipeline Type
- **Batch Pipeline**: Runs every 8 hours via **Cloud Scheduler**
- Chosen due to the daily update nature of the GISTDA API

---

## 🔐 Security Best Practices
- Environment variables managed via `.env` (not committed)
- Secrets (API keys, project info) stored outside Git
- No hardcoded credentials

---

## 🤝 Reproducibility
### Requirements
- GCP Project
- Enable APIs: Cloud Run, BigQuery, GCS, Pub/Sub, Scheduler, Places API

### Deploy
1. Fork this repo
2. Set up `.env` in each service folder:
```bash
PROJECT_ID=your_project
BUCKET_NAME=your_bucket
DATASET_ID=FireMai
TABLE_ID=fire_data
TABLE_ID_NEARBY=fire_data_nearby_places
GOOGLE_API_KEY=your_google_places_api_key
```
3. Deploy each Cloud Run service using Cloud Console or Cloud Build
4. Configure Pub/Sub triggers and GCS notification for FireMai folder
5. Create Cloud Scheduler job to trigger `load_api_to_gcs`

---

## 👀 Dashboard
[View Looker Studio Dashboard](https://lookerstudio.google.com/)

---

## ✅ Final Note
This project is designed to be robust, modular, and refreshable. The GISTDA API can be changed or updated and the entire pipeline will adapt.

> Let's protect Thailand's forests 🌳❤️