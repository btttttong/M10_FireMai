from fastapi import FastAPI, Request
import requests, json, os, base64
import pandas as pd
from google.cloud import bigquery
from datetime import datetime

app = FastAPI()

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID")
TABLE_ID = os.getenv("TABLE_ID_NEARBY")

NEARBY_TYPES = ["school", "hospital", "place_of_worship"]
RADIUS = 3000  # in meters

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
                "open_now": place.get("opening_hours", {}).get("open_now"),
                "business_status": place.get("business_status")
            })
    return all_results

def enrich_and_store(hotspot):
    lat, lng = hotspot["latitude"], hotspot["longitude"]
    hotspotid = hotspot["hotspotid"]
    acq_date = hotspot["acq_date"]

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
            **place
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    if df.empty:
        return

    client = bigquery.Client()
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"✅ Uploaded {len(df)} nearby places for hotspot {hotspotid}")

@app.post("/")
async def handle_pubsub(request: Request):
    body = await request.json()
    message = body.get("message", {})
    data = message.get("data")
    if not data:
        return {"error": "No data in message"}, 400

    decoded = base64.b64decode(data).decode("utf-8")
    hotspot = json.loads(decoded)
    print("📍 Processing hotspot:", hotspot.get("hotspotid"))
    enrich_and_store(hotspot)
    return {"status": "success"}
