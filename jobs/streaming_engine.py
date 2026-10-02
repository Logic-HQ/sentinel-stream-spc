"""
SentinelStream Core Engine.
Handles the lifecycle of the SparkSession, reads from Kafka,
orchestrates modular transformations, and routes outputs to target sinks.
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Import your modular transformations (to be built next in transforms/)
from transforms.spc_metrics import calculate_spc_limits
from transforms.z_score_eval import evaluate_z_scores
from transforms.isolation_forest import detect_drift_anomalies

logger = logging.getLogger("sentinel-stream-engine")

def get_spark_session() -> SparkSession:
    """Initializes and returns a configured SparkSession with streaming packages."""
    return SparkSession.builder \
        .appName("SentinelStream-Engine") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

def get_raw_iot_schema() -> StructType:
    """Defines the strict incoming schema configuration for payload validation."""
    return StructType([
        StructField("sensor_id", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("reading", DoubleType(), False),
        StructField("metric_type", StringType(), True)
    ])

def run_pipeline(brokers: str, input_topic: str, output_topic: str, dlq_topic: str, checkpoint_dir: str):
    """Orchestrates the end-to-end streaming data pipeline."""
    spark = get_spark_session()
    logger.info("Spark session successfully established.")

    # 1. Ingestion: Read from Kafka Stream
    logger.info(f"Subscribing to Kafka topic stream: {input_topic}")
    raw_kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", brokers) \
        .option("subscribe", input_topic) \
        .option("startingOffsets", "latest") \
        .load()

    # 2. Schema Parse & Initial Validation
    # Cast binary payload to string and parse JSON against defined schema
    iot_schema = get_raw_iot_schema()
    parsed_stream = raw_kafka_stream \
        .selectExpr("CAST(value AS STRING) as json_payload", "timestamp as kafka_received_at") \
        .select(
            from_json(col("json_payload"), iot_schema).alias("data"),
            col("json_payload"),
            col("kafka_received_at")
        ) \
        .select("data.*", "json_payload", "kafka_received_at")

    # 3. Route Corrupt Payloads to Dead Letter Queue (DLQ)
    # Check if critical fields are parsed as null due to structural corruption
    corrupt_stream = parsed_stream.filter(
        col("sensor_id").isNull() | col("reading").isNull()
    ).select(
        col("json_payload").alias("value")  # Keep original corrupted string for debugging
    )

    # 4. Process Clean Validated Stream through Business Logic
    valid_base_stream = parsed_stream.filter(
        col("sensor_id").isNotNull() & col("reading").isNotNull()
    )

    # Task 1: Calculate moving window statistics and baseline SPC limits
    spc_enriched_stream = calculate_spc_limits(valid_base_stream)

    # Task 2: Perform Real-time Z-score evaluation against statistical boundaries
    z_score_stream = evaluate_z_scores(spc_enriched_stream)

    # Task 3: Apply ML Micro-batch modeling via Isolation Forest for advanced drift detection
    # This invokes a micro-batch level structural write or mapGroupsWithState
    final_processed_stream = detect_drift_anomalies(z_score_stream)

    # Prepare outbound clean payload structure mapped into JSON for Kafka distribution
    clean_kafka_payload = final_processed_stream.select(
        to_json(struct([col(c) for c in final_processed_stream.columns])).alias("value")
    )

    # 5. Execution Sinks: Multi-routing Write Operations
    logger.info("Initializing active continuous streaming target writers...")

    # Write clean enriched streams to operational time-series engine / downstream topic
    clean_query = clean_kafka_payload.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", brokers) \
        .option("topic", output_topic) \
        .option("checkpointLocation", f"{checkpoint_dir}/clean_metrics") \
        .outputMode("append") \
        .start()

    # Write corrupt/failed structural schemas safely to Dead Letter Queue
    dlq_query = corrupt_stream.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", brokers) \
        .option("topic", dlq_topic) \
        .option("checkpointLocation", f"{checkpoint_dir}/dlq_metrics") \
        .outputMode("append") \
        .start()

    # Block current thread until streaming context termination signal
    spark.streams.awaitAnyTermination()
