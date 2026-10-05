from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def calculate_windowed_spc_limits(
    df: DataFrame, 
    window_duration: str = "5 minutes", 
    slide_duration: str = "1 minute"
) -> DataFrame:
    """
    Calculates real-time moving window statistical process control (SPC) limits.
    
    Expects input schema to have:
        - timestamp: TimestampType
        - metric_name: StringType
        - metric_value: DoubleType
    """
    # 1. Group by window and metric type to calculate historical baseline stats
    windowed_stats = (
        df.groupBy(
            F.window(F.col("timestamp"), window_duration, slide_duration),
            F.col("metric_name")
        )
        .agg(
            F.avg("metric_value").alias("moving_avg"),
            F.stddev("metric_value").alias("moving_stddev"),
            F.count("metric_value").alias("sample_count")
        )
        # Handle early windows with only 1 observation to avoid null standard deviations
        .na.fill(value=0.0, subset=["moving_stddev"]) 
    )
    
    # 2. Derive Standard 3-Sigma Statistical Process Control Limits
    spc_limits = windowed_stats.withColumns({
        "lcl": F.col("moving_avg") - (3 * F.col("moving_stddev")),
        "ucl": F.col("moving_avg") + (3 * F.col("moving_stddev"))
    })
    
    return spc_limits
