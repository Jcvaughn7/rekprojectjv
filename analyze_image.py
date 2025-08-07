import os
import json
from decimal import Decimal
from datetime import datetime
import boto3

# -------------------------------------------------------------------
# 1. Get AWS config from environment variables (with safe defaults)
# -------------------------------------------------------------------
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")  # fallback if not set
S3_BUCKET = os.getenv("S3_BUCKET")
DYNAMODB_TABLE_BETA = os.getenv("DYNAMODB_TABLE_BETA")
DYNAMODB_TABLE_PROD = os.getenv("DYNAMODB_TABLE_PROD")

# -------------------------------------------------------------------
# 2. Create AWS clients with explicit region
# -------------------------------------------------------------------
s3 = boto3.client("s3", region_name=AWS_REGION)
rekognition = boto3.client("rekognition", region_name=AWS_REGION)
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)

# -------------------------------------------------------------------
# 3. Upload file to S3
# -------------------------------------------------------------------
def upload_image_to_s3(file_path, bucket, key):
    try:
        s3.upload_file(file_path, bucket, key)
        print(f"Uploaded {file_path} to s3://{bucket}/{key}")
    except Exception as e:
        print(f"❌ S3 Upload Failed: {e}")
        raise

# -------------------------------------------------------------------
# 4. Analyze image with Rekognition
# -------------------------------------------------------------------
def analyze_image(bucket, key):
    try:
        response = rekognition.detect_labels(
            Image={"S3Object": {"Bucket": bucket, "Name": key}},
            MaxLabels=10,
            MinConfidence=70
        )
        labels = [
            {"Name": label["Name"], "Confidence": Decimal(str(label["Confidence"]))}
            for label in response["Labels"]
        ]
        return labels
    except Exception as e:
        print(f"❌ Rekognition Analysis Failed: {e}")
        raise

# -------------------------------------------------------------------
# 5. Write results to DynamoDB
# -------------------------------------------------------------------
def write_to_dynamodb(table_name, file_name, labels, branch):
    try:
        table = dynamodb.Table(table_name)
        item = {
            "filename": file_name,
            "labels": labels,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "branch": branch
        }
        table.put_item(Item=item)
        print(f"✅ Data written to DynamoDB table: {table_name}")
    except Exception as e:
        print(f"❌ DynamoDB Write Failed: {e}")
        raise

# -------------------------------------------------------------------
# 6. Main Execution
# -------------------------------------------------------------------
if __name__ == "__main__":
    # Image path (update if needed)
    image_path = os.path.join("images", "birds.jpg.jpg")
    file_name = os.path.basename(image_path)
    s3_key = f"rekognition-input/{file_name}"

    # Detect Git branch from GitHub Actions env, else fallback
    branch_name = os.getenv("GITHUB_HEAD_REF") or os.getenv("GITHUB_REF_NAME") or "local"

    # Choose correct DynamoDB table based on branch
    if branch_name == "main":
        table_name = DYNAMODB_TABLE_PROD
    else:
        table_name = DYNAMODB_TABLE_BETA

    # Validate required env vars
    if not all([AWS_REGION, S3_BUCKET, table_name]):
        raise EnvironmentError("❌ Missing one or more required environment variables.")

    # Run pipeline
    upload_image_to_s3(image_path, S3_BUCKET, s3_key)
    labels = analyze_image(S3_BUCKET, s3_key)
    write_to_dynamodb(table_name, s3_key, labels, branch_name)

    print("🎯 Image analysis pipeline complete.")
