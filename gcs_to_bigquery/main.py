import pandas as pd
import json
import os
from google.cloud import storage, bigquery
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
BUCKET_NAME = os.getenv("BUCKET_NAME")
FOLDER = os.getenv("SUBFOLDER", "FireMai")
DATASET_ID = os.getenv("DATASET_ID")
TABLE_ID = os.getenv("TABLE_ID")

def get_latest_file(bucket_name, folder):
    client = storage.Client()
    blobs = list(client.list_blobs(bucket_name, prefix=folder))
    json_blobs = [b for b in blobs if b.name.endswith(".json")]
    if not json_blobs:
        raise Exception("❌ No JSON files found.")
    latest_blob = max(json_blobs, key=lambda b: b.updated)
    print(f"📄 Latest file: {latest_blob.name}")
    return latest_blob

def load_json_from_blob(blob):
    content = blob.download_as_text()
    data = json.loads(content)
    features = data.get("features", [])
    return pd.DataFrame([f["properties"] for f in features])

def create_unique_key(df):
    return (
        df["hotspotid"].astype(str) + "_" +
        df["acq_date"].astype(str) + "_" +
        df["latitude"].astype(str) + "_" +
        df["longitude"].astype(str)
    )

def filter_new_records(df):
    client = bigquery.Client(project=PROJECT_ID)
    query = f"SELECT unique_key FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`"
    try:
        existing = client.query(query).to_dataframe()
        df["unique_key"] = create_unique_key(df)
        new_df = df[~df["unique_key"].isin(existing["unique_key"])]
        return new_df.drop(columns=["unique_key"])
    except Exception as e:
        print("⚠️ Error or no table found:", e)
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

if __name__ == "__main__":
    blob = get_latest_file(BUCKET_NAME, FOLDER)
    df = load_json_from_blob(blob)
    print(f"📦 Fetched {df.shape[0]} records from GCS.")
    if not df.empty:
        new_df = filter_new_records(df)
        print(f"🆕 New records: {new_df.shape[0]}")
        if not new_df.empty:
            upload_to_bigquery(new_df)
        else:
            print("✅ No new records to upload.")
