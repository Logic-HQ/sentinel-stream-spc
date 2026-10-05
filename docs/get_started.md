Network Architecture Notes for PySpark
• If PySpark is running inside Docker: It should connect to Kafka using kafka:9092 and ClickHouse using clickhouse:8123.
• If PySpark is running locally on your host machine (outside Docker): It should connect to Kafka using localhost:9094 and ClickHouse using localhost:8123.

⚠️  (Crucial Step: Make the script executable on your host system before launching Docker by running: chmod +x deployments/init-infra.sh)

As new init-infra container block to your existing docker-compose.yml file. This service waits for kafka and minio to pass their respective health checks, maps the configuration script, runs it, and then gracefully exits.

1. Running docker-compose up -d boots up Kafka and MinIO.
2. The init-infra service idles until Kafka's internal cluster check and MinIO's HTTP endpoints signal they are fully accepting operations.
3. The provisioner downloads the static MinIO client (mc), attaches it to the local instance, runs your topic/bucket definitions, and automatically shuts itself down (Exit 0), keeping your local environment clean and automated.

Enhancement Tips
• Watermarking: The .withWatermark("timestamp", "10 minutes") function is critical. Tell hiring managers you added this to prevent infinite state memory accumulation in memory, allowing Spark to drop state tracking for old data windows.
• Stream-to-Stream Join: Doing a temporal boundary join (timestamp >= window.start AND timestamp < window.end) evaluates real-time telemetry events against their concurrent rolling baseline cohort seamlessly.


ClickHouse is built for fast analytical queries over massive time-series datasets. For an SPC project, you want to store your clean telemetry separately or in a highly indexed format so Grafana can query rolling standard deviations instantly without slowing down.

• LowCardinality Design: Applied to columns like metric_name and device_id to heavily compress repetitious string keys, reducing disk I/O significantly.
• Ordering Key Choice: Ordering by (metric_name, device_id, timestamp) aligns perfectly with how you query windowed historical metrics for statistical checks.


In this design, point out that mapInPandas requires Apache Arrow optimization execution configurations. Mention that you enabled Arrow serialization in your configuration (spark.sql.execution.arrow.pyspark.enabled: "true") to bypass Java-Python IPC bottlenecks during machine learning evaluations.


## Python Data Generator 
Next to pump synthetic data (and intentionally inject anomalies) into Kafka to test this.

src/producer/generator.py

This script generates realistic synthetic IoT sensor telemetry (such as temperature and vibration) using a clean baseline sine wave. Crucially, it includes an automated loop that periodically injects three distinct anomaly patterns corresponding precisely to your pipeline's three analytical tests.

### Verification Checklist

Once your whole framework structure is laid down, running it end-to-end takes exactly three terminal tabs:
1. Tab 1 (Infrastructure): Run docker-compose up -d to spin up everything and let the init-infra image automatically establish your iot-sensor-stream topic.
2. Tab 2 (The PySpark Core Engine): Execute your src/streaming/main.py streaming script.
3. Tab 3 (The Telemetry Simulator): Run python src/producer/generator.py.
You will immediately see normal logs running in the console, followed every few minutes by highlighted ⚠️ [INJECTED ...] events, which you can immediately track landing inside your Kafka-UI container (http://localhost:8080) or routing directly to the target dead-letter-queue topic!


## Implementation of Consumer for the MinIO Dead Letter Queue (DLQ) 

This script listens continuously to the dead-letter-queue Kafka topic where your PySpark pipeline automatically dumps flagged anomalies. It micro-batches these alerts and uploads them as timestamped .json objects directly to your MinIO S3 bucket archive for long-term audit logs.


The DLQ to S3 Consumer Script
src/producer/dlq_consumer.py

