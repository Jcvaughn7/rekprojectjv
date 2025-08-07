import boto3
import os
import json
from datetime import datetime, timezone
from decimal import Decimal  # <-- ADD THIS

def upload_image_to_s3(file_path, bucket, key):
    s3 = boto3.client('s3')
    s3.upload_file(file_path, bucket, key)
    print(f"Uploaded {file_path} to s3://{bucket}/{key}")

def analyze_image(bucket, key):
    rekognition = boto3.client('rekognition')
    response = rekognition.detect_labels(
        Image={'S3Object': {'Bucket': bucket, 'Name': key}},
        MaxLabels=10
    )
    # Convert floats to Decimal
    labels = [
        {"Name": lbl["Name"], "Confidence": Decimal(str(round(lbl["Confidence"], 2)))}
        for lbl in response["Labels"]
    ]
    return labels

def write_to_dynamodb(table_name, filename, labels, branch):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    timestamp = datetime.now(timezone.utc).isoformat()
    item = {
        "filename": filename,
        "labels": labels,
        "timestamp": timestamp,
        "branch": branch
    }
    table.put_item(Item=item)
    print(f"Inserted result for {filename} into DynamoDB table {table_name}")

if __name__ == "__main__":
    # Strip whitespace from AWS creds if needed
    if os.getenv("AWS_ACCESS_KEY_ID"):
        os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("AWS_ACCESS_KEY_ID").strip()
    if os.getenv("AWS_SECRET_ACCESS_KEY"):
        os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("AWS_SECRET_ACCESS_KEY").strip()

    bucket_name = os.getenv("S3_BUCKET")
    branch_name = os.getenv("GITHUB_HEAD_REF") or os.getenv("GITHUB_REF_NAME") or "local"
    table_name = os.getenv("DYNAMODB_TABLE")
    prefix = "rekognition-input/"

    # Pick first image in images/
    import glob
    images = glob.glob("images/*.jpg") + glob.glob("images/*.png")
    if not images:
        raise FileNotFoundError("No images found in images/ folder.")

    for image_path in images:
        file_name = os.path.basename(image_path)
        s3_key = f"{prefix}{file_name}"

        # Upload
        upload_image_to_s3(image_path, bucket_name, s3_key)

        # Analyze
        labels = analyze_image(bucket_name, s3_key)

        # Save to DynamoDB
        write_to_dynamodb(table_name, f"{prefix}{file_name}", labels, branch_name)
