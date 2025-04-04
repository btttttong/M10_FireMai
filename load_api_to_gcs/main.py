from fastapi import FastAPI, Request
from google.cloud import storage
from datetime import datetime
import requests, json, time, os
import uvicorn

app = FastAPI()

API_URL = "https://disaster.gistda.or.th/api/1.0/documents/fire/hotspot/modis/30days"
API_KEY = os.getenv("API_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")
SUBFOLDER = os.getenv("SUBFOLDER", "FireMai")
HEADERS = {"API-Key": API_KEY}

def fetch_all_data():
    all_data = []
    offset = 0
    while True:
        params = {"limit": 1000, "offset": offset, "ct_tn": "ราชอาณาจักรไทย"}
        r = requests.get(API_URL, headers=HEADERS, params=params)
        print(f"📥 Fetching offset {offset}...")
        r.raise_for_status()
        features = r.json().get("features", [])
        if not features:
            print("✅ No more data.")
            break
        all_data.extend(features)
        offset += 1000
        time.sleep(0.5)
    return {"features": all_data}

def upload_to_gcs(data):
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    filename = f"{SUBFOLDER}/hotspot_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.json"
    blob = bucket.blob(filename)
    blob.upload_from_string(json.dumps(data), content_type="application/json")
    print(f"✅ Uploaded to GCS: {filename}")

@app.get("/")
@app.post("/")
async def run_pipeline(request: Request):
    try:
        data = fetch_all_data()
        upload_to_gcs(data)
        return {"message": "✅ Upload completed"}
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
