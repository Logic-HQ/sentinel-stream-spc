#!/bin/bash
set -e

echo "🚀 Starting SentinelStream infrastructure initialization..."

# ---------------------------------------------------------------------------
# 1. PROVISION KAFKA TOPICS
# ---------------------------------------------------------------------------
echo "📥 Creating Kafka Topics..."

# Main stream ingestion topic
kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists \
  --topic iot-sensor-stream \
  --partitions 3 \
  --replication-factor 1

# Dead Letter Queue (DLQ) topic for malformed payloads or severe anomalies
kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists \
  --topic dead-letter-queue \
  --partitions 1 \
  --replication-factor 1

echo "✅ Kafka topics created successfully."

# ---------------------------------------------------------------------------
# 2. PROVISION MINIO BUCKETS
# ---------------------------------------------------------------------------
echo "🪣 Creating MinIO S3 Buckets..."

# Configure the MinIO Client (mc) alias using internal cluster endpoint
mc alias set sentinel_minio http://minio:9000 sentinel_admin sentinel_password

# Create bucket for streaming DLQ log/alerts data archive
if ! mc ls sentinel_minio/sentinel-dlq-archive > /dev/null 2>&1; then
  mc mb sentinel_minio/sentinel-dlq-archive
  echo "✅ MinIO bucket 'sentinel-dlq-archive' created."
else
  echo "ℹ️ MinIO bucket 'sentinel-dlq-archive' already exists."
fi

echo "🎉 Infrastructure provisioning complete! Ready for SentinelStream ingestion."
