from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from tasks.spc_limits import calculate_windowed_spc_limits
from tasks.z_score import evaluate_z_scores
from tasks.isolation_forest import evaluate_multivariate_anomalies


from tasks.spc_limits import calculate_windowed_spc_limits

def main():
    # Initialize Spark Session with Kafka ingestion packages
    spark = SparkSession.builder \
        .appName("SentinelStream-SPC-Engine") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    # Define strict incoming IoT schema payload
    sensor_schema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("device_id", StringType(), True),
        StructField("metric_name", StringType(), True),
        StructField("metric_value", DoubleType(), True)
    ])

    # 1. Read Stream from Ingested Kafka Topic
    kafka_raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9094") \
        .option("subscribe", "iot-sensor-stream") \
        .option("startingOffsets", "latest") \
        .load()

    # 2. Parse JSON string value column matching schema
    parsed_stream = kafka_raw_stream \
        .selectExpr("CAST(value AS STRING) as json_payload") \
        .select(F.from_json(F.col("json_payload"), sensor_schema).alias("data")) \
        .select("data.*") \
        .withWatermark("timestamp", "10 minutes")  # Crucial for state clean up in streaming windows

    # 3. Task 1: Compute real-time moving baselines & limits
    spc_limits_stream = calculate_windowed_spc_limits(
        parsed_stream, 
        window_duration="5 minutes", 
        slide_duration="1 minute"
    )

    # 4. Evaluate current raw stream records against calculated window limits
    # Join on matching time window boundaries and metric category
    enriched_stream = parsed_stream.join(
        spc_limits_stream,
        expr="""
            metric_name = metric_name AND
            timestamp >= window.start AND
            timestamp < window.end
        """
    ).withColumn(
        "is_spc_anomaly",
        F.when((F.col("metric_value") > F.col("ucl")) | (F.col("metric_value") < F.col("lcl")), 1)
        .otherwise(0)
    )

    # 5. Output results to terminal for local debugging
    query = enriched_stream.writeStream \
        .format("console") \
        .outputMode("append") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()

# Insert this directly into your existing src/streaming/main.py flow right below Task 1:
from tasks.z_score import evaluate_z_scores

# ... (Previous steps from Task 1 streaming layout) ...

# 4. Task 2 Join & Z-Score validation execution
enriched_stream = parsed_stream.join(
    spc_limits_stream,
    expr="""
        metric_name = metric_name AND
        timestamp >= window.start AND
        timestamp < window.end
    """
)

# Apply dynamic point-in-time standard evaluation metrics
validated_stream = evaluate_z_scores(
    enriched_stream,
    historical_mean_col="moving_avg",
    historical_stddev_col="moving_stddev",
    z_threshold=3.5
)

# 5. Route validated streams to ClickHouse Time-Series DB Sink
def write_to_clickhouse(batch_df: DataFrame, batch_id: int):
    # Flatten or transform specific fields if necessary to match ClickHouse SQL exact structures
    # Using clickhouse-connect or standard JDBC drivers to flush multi-rows instantly
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:clickhouse://localhost:8123/sentinel_db") \
        .option("dbtable", "sensor_metrics") \
        .option("user", "sentinel_user") \
        .option("password", "sentinel_password") \
        .mode("append") \
        .save()

# Spark Streaming Action Sink Trigger
ch_query = validated_stream.writeStream \
    .foreachBatch(write_to_clickhouse) \
    .option("checkpointLocation", "/tmp/spark_checkpoints/clickhouse_sink") \
    .start()

from tasks.spc_limits import calculate_windowed_spc_limits
from tasks.z_score import evaluate_z_scores
from tasks.isolation_forest import evaluate_multivariate_anomalies

# Kafka ingestion steps & Task 1 / Task 2 logic

# Apply dynamic point-in-time standard evaluation metrics (Task 2 Output)
validated_stream = evaluate_z_scores(
    enriched_stream,
    historical_mean_col="moving_avg",
    historical_stddev_col="moving_stddev"
).withColumn("is_spc_anomaly", F.col("is_spc_anomaly").cast("long")) # cast for schema alignment

# ---------------------------------------------------------------------------
# Task 3: Multivariate Isolation Forest Execution via mapInPandas
# ---------------------------------------------------------------------------
fully_evaluated_stream = evaluate_multivariate_anomalies(validated_stream)

# ---------------------------------------------------------------------------
# Split clean payload streams from Dead Letter Queue (DLQ) anomalies
# ---------------------------------------------------------------------------
# If flagged by any of our 3 statistical models, redirect to DLQ routing condition
dlq_condition = (
    (F.col("is_spc_anomaly") == 1) | 
    (F.col("is_z_score_outlier") == 1) | 
    (F.col("is_iforest_anomaly") == 1)
)

anomalous_stream = fully_evaluated_stream.filter(dlq_condition)
clean_stream = fully_evaluated_stream.filter(~dlq_condition)

# ---------------------------------------------------------------------------
# Pipeline Sink: Route Anomalies to Kafka Dead Letter Queue (DLQ Topic)
# ---------------------------------------------------------------------------
dlq_query = anomalous_stream \
    .selectExpr("CAST(device_id AS STRING) AS key", "to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9094") \
    .option("topic", "dead-letter-queue") \
    .option("checkpointLocation", "/tmp/spark_checkpoints/dlq_sink") \
    .start()

# ---------------------------------------------------------------------------
# Pipeline Sink: Route Clean Data to ClickHouse Analytics Database
# ---------------------------------------------------------------------------
def write_clean_to_clickhouse(batch_df: DataFrame, batch_id: int):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:clickhouse://localhost:8123/sentinel_db") \
        .option("dbtable", "sensor_metrics") \
        .option("user", "sentinel_user") \
        .option("password", "sentinel_password") \
        .mode("append") \
        .save()

clickhouse_query = clean_stream.writeStream \
    .foreachBatch(write_clean_to_clickhouse) \
    .option("checkpointLocation", "/tmp/spark_checkpoints/clickhouse_clean_sink") \
    .start()

spark.streams.awaitAnyTermination()
