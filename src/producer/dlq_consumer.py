import json
import os
from datetime import datetime, timezone
import boto3
from botocore.client import Config
from kafka import KafkaConsumer

# ---------------------------------------------------------------------------
# CONFIGURATION PARAMETERS
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9094']
DLQ_TOPIC = 'dead-letter-queue'
CONSUMER_GROUP = 'sentinel-dlq-archiver'

MINIO_ENDPOINT = 'http://localhost:9000'
MINIO_ACCESS_KEY = 'sentinel_admin'
MINIO_SECRET_KEY = 'sentinel_password'
BUCKET_NAME = 'sentinel-dlq-archive'

# Initialize MinIO S3 Client via Boto3
s3_client = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version='s3v4'),
    region_name='us-east-1' # Default filler region for local setups
)

# Initialize Kafka Consumer for DLQ
consumer = KafkaConsumer(
    DLQ_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id=CONSUMER_GROUP,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_serializer=lambda v: v.decode('utf-8')
)

print(f"📥 DLQ Consumer active. Monitoring topic '{DLQ_TOPIC}'...")
print(f"🪣 Archiving anomaly records to MinIO S3 bucket '{BUCKET_NAME}'...")

# ---------------------------------------------------------------------------
# CONSUMPTION LOOP & ARCHIVE PIPELINE
# ---------------------------------------------------------------------------
try:
    for message in consumer:
        try:
            # Parse the payload passed by PySpark
            payload_str = message.value
            payload_json = json.loads(payload_str)
            
            # Extract key details to construct an organized Hive-style S3 path layout
            device_id = payload_json.get("device_id", "unknown_device")
            metric_name = payload_json.get("metric_name", "unknown_metric")
            
            # Create a high-resolution execution timestamp for file naming
            now = datetime.now(timezone.utc)
            timestamp_str = now.strftime("%Y%m%d-%H%M%S-%f")
            
            # Construct partitioned Object Storage Key
            # Format: sentinel-dlq-archive/year=YYYY/month=MM/day=DD/device/metric_alert_timestamp.json
            s3_key = (
                f"year={now.strftime('%Y')}/"
                f"month={now.strftime('%m')}/"
                f"day={now.strftime('%d')}/"
                f"{device_id}/{metric_name}_alert_{timestamp_str}.json"
            )
            
            # Upload the raw error data directly as an S3 object string stream
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=s3_key,
                Body=json.dumps(payload_json, indent=2),
                ContentType='application/json'
            )
            
            print(f"✅ Archived Anomaly Log to S3: s3://{BUCKET_NAME}/{s3_key}")
            
        except json.JSONDecodeError:
            print(f"❌ Received unparseable malformed payload on DLQ: {message.value}")
        except Exception as e:
            print(f"❌ Failed to archive record to MinIO: {str(e)}")

except KeyboardInterrupt:
    print("\n🛑 DLQ S3 Archiver shutting down gracefully.")
