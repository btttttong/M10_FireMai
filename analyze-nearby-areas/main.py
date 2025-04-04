from fastapi import FastAPI, Request
import requests, json, os, base64
import pandas as pd
from google.cloud import bigquery

app = FastAPI()

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID")
TABLE_ID = os.getenv("TABLE_ID_NEARBY")

NEARBY_TYPES = ["school", "hospital", "place_of_worship"]
RADIUS = 3000  # meters

def get_nearby_places(lat, lng):
    all_results = []
    for place_type in NEARBY_TYPES:
        url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
        params = {
            "location": f"{lat},{lng}",
            "radius": RADIUS,
            "type": place_type,
            "key": GOOGLE_API_KEY
        }
        res = requests.get(url, params=params)
        if res.status_code != 200:
            continue
        for place in res.json().get("results", []):
            all_results.append({
                "place_type": place_type,
                "place_id": place.get("place_id"),
                "name": place.get("name"),
                "lat": place["geometry"]["location"]["lat"],
                "lng": place["geometry"]["location"]["lng"],
                "vicinity": place.get("vicinity"),
                "types": place.get("types"),
                "rating": place.get("rating"),
                "user_ratings_total": place.get("user_ratings_total"),
                "open_now": bool(place.get("opening_hours", {}).get("open_now")) if place.get("opening_hours") else None,
                "business_status": place.get("business_status")
            })
    return all_results

def enrich_and_store(hotspot):
    lat, lng = hotspot["latitude"], hotspot["longitude"]
    hotspotid = hotspot["hotspotid"]
    acq_date = hotspot["acq_date"]
    pv_en = hotspot.get("pv_en")  # ✅ province EN
    pv_tn = hotspot.get("pv_tn")  # ✅ province TH

    places = get_nearby_places(lat, lng)
    if not places:
        return

    rows = []
    for place in places:
        row = {
            "hotspotid": hotspotid,
            "acq_date": acq_date,
            "latitude": lat,
            "longitude": lng,
            "pv_en": pv_en,
            "pv_tn": pv_tn,
            **place
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    if df.empty:
        return
    
    df["open_now"] = df["open_now"].apply(lambda x: 1 if x is True else (0 if x is False else None)).astype("Int64")

    client = bigquery.Client()
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"✅ Uploaded {len(df)} nearby places for hotspot {hotspotid}")

@app.get("/")
def run_enrichment():
    client = bigquery.Client()
    query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET_ID}.fire_data`
        WHERE DATE(acq_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    """
    df = client.query(query).to_dataframe()
    print(f"🕵️ Queried {len(df)} fire records")

    for _, row in df.iterrows():
        enrich_and_store(row.to_dict())

    return {"status": "✅ Done"}