"""
SentinelStream Transformation Module - Task 1.
Computes real-time rolling window metrics (Moving Average, Moving StdDev)
to dynamically establish Statistical Process Control (SPC) thresholds.
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, window, avg, stddev, expr

logger = logging.getLogger("sentinel-stream-spc")

def calculate_spc_limits(df: DataFrame, window_duration: str = "5 minutes", slide_duration: str = "1 minute") -> DataFrame:
    """
    Calculates rolling statistical baselines using a time-series sliding window.
    
    Args:
        df (DataFrame): Clean incoming validated DataFrame stream.
        window_duration (str): Length of the rolling time window.
        slide_duration (str): Frequency of window updates.
        
    Returns:
        DataFrame: Stream joined with its dynamic historical SPC limits.
    """
    logger.info(f"Applying rolling SPC limits window: length={window_duration}, slide={slide_duration}")

    # 1. Define Watermark to handle late/out-of-order data and clear Spark state memory
    watermarked_df = df.withWatermark("timestamp", "10 minutes")

    # 2. Compute stateful window aggregates per sensor/metric type
    windowed_stats = watermarked_df \
        .groupBy(
            window(col("timestamp"), window_duration, slide_duration),
            col("sensor_id"),
            col("metric_type")
        ) \
        .agg(
            avg("reading").alias("moving_avg"),
            stddev("reading").alias("moving_stddev")
        ) \
        .select(
            col("window.start").alias("win_start"),
            col("window.end").alias("win_end"),
            col("sensor_id").alias("stat_sensor_id"),
            col("metric_type").alias("stat_metric_type"),
            col("moving_avg"),
            # Coalesce to handle early edge-cases where 1 single data point yields NaN StdDev
            expr("coalesce(moving_stddev, 0.0)").alias("moving_stddev")
        )

    # 3. Stream-Stream Join back to raw events to append runtime control baselines
    # Events are matched if they fall inside the historical processing window frame
    enriched_stream = watermarked_df.join(
        windowed_stats,
        expr("""
            sensor_id = stat_sensor_id AND
            metric_type = stat_metric_type AND
            timestamp >= win_start AND
            timestamp < win_end
        """),
        joinType="inner"
    ).drop("stat_sensor_id", "stat_metric_type")

    # 4. Calculate typical 3-Sigma SPC Threshold Limits
    final_spc_stream = enriched_stream \
        .withColumn("ucl", col("moving_avg") + (col("moving_stddev") * 3.0)) \
        .withColumn("lcl", col("moving_avg") - (col("moving_stddev") * 3.0))

    return final_spc_stream
