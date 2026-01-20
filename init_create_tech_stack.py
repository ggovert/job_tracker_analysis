import boto3
import json
import os
from botocore.client import Config
import os
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
RUSTFS_ENDPOINT = "http://localhost:9000" # Use 'http://rustfs:9000' if running inside Docker
ACCESS_KEY = os.getenv("RUSTFS_ACCESS_KEY")
SECRET_KEY = os.getenv("RUSTFS_SECRET_KEY")
METADATA_BUCKET = "metadata"
MASTER_FILE_PATH = "standardized_tech_stack.json" # The file on your local machine

# 1. Initialize S3 Client
s3 = boto3.client(
    "s3",
    endpoint_url=RUSTFS_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1"
)

def setup_metadata_bucket():
    # 2. Create Bucket if not exists
    try:
        s3.head_bucket(Bucket=METADATA_BUCKET)
        print(f"✅ Bucket '{METADATA_BUCKET}' already exists.")
    except:
        s3.create_bucket(Bucket=METADATA_BUCKET)
        print(f"🚀 Created new bucket: '{METADATA_BUCKET}'")

    # 3. Upload the Master List
    if os.path.exists(MASTER_FILE_PATH):
        try:
            with open(MASTER_FILE_PATH, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            s3.put_object(
                Bucket=METADATA_BUCKET,
                Key="standardized_tech_stack.json",
                Body=json.dumps(data, indent=4).encode("utf-8"),
                ContentType="application/json"
            )
            print(f"✅ Master list (Ver 1) uploaded to {METADATA_BUCKET}/standardized_tech_stack.json")
        except Exception as e:
            print(f"❌ Failed to upload master list: {e}")
    else:
        print(f"⚠️ Error: Local file '{MASTER_FILE_PATH}' not found. Please check the path.")

if __name__ == "__main__":
    setup_metadata_bucket()