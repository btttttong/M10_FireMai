from fastapi import FastAPI, Request
from google.cloud import storage, bigquery
import pandas as pd
import json, os, base64

app = FastAPI()

PROJECT_ID = os.getenv("PROJECT_ID")
BUCKET_NAME = os.getenv("BUCKET_NAME")
FOLDER = os.getenv("SUBFOLDER", "FireMai")
DATASET_ID = os.getenv("DATASET_ID")
TABLE_ID = os.getenv("TABLE_ID")

def create_unique_key(df):
    return (
        df["hotspotid"].astype(str) + "_" +
        df["acq_date"].astype(str) + "_" +
        df["latitude"].astype(str) + "_" +
        df["longitude"].astype(str)
    )

def load_json_from_blob(blob_name):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_name)
    content = blob.download_as_text()
    data = json.loads(content)
    features = data.get("features", [])
    return pd.DataFrame([f["properties"] for f in features])

def filter_new_records(df):
    client = bigquery.Client(project=PROJECT_ID)
    query = f"SELECT unique_key FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`"

    try:
        existing = client.query(query).to_dataframe()
        df = df.fillna("")  # ✅ Prevent NaN in key
        df["unique_key"] = create_unique_key(df)
        print("✅ Total incoming:", len(df))
        print("✅ Found existing keys:", len(existing))

        new_df = df[~df["unique_key"].isin(existing["unique_key"])]
        print("🆕 New records:", len(new_df))

        return new_df.drop(columns=["unique_key"])
    except Exception as e:
        print("⚠️ Error in dedup:", e)
        df = df.fillna("")
        df["unique_key"] = create_unique_key(df)
        return df.drop(columns=["unique_key"])

def upload_to_bigquery(df):
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True
    )
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"✅ Uploaded {df.shape[0]} new rows to {table_ref}")

@app.post("/")
async def pubsub_trigger(request: Request):
    try:
        body = await request.json()
        message = body.get("message", {})
        data_b64 = message.get("data")
        if data_b64:
            decoded = base64.b64decode(data_b64).decode("utf-8")
            event = json.loads(decoded)
            blob_name = event["name"]
            print(f"📄 Triggered by file: {blob_name}")
            df = load_json_from_blob(blob_name)
            if not df.empty:
                new_df = filter_new_records(df)
                if not new_df.empty:
                    upload_to_bigquery(new_df)
            return {"status": "✅ Success"}
        else:
            return {"error": "No data in message"}, 400
    except Exception as e:
        return {"error": str(e)}, 500
