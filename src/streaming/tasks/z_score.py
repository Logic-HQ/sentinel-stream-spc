from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def evaluate_z_scores(
    df: DataFrame, 
    historical_mean_col: str = "moving_avg", 
    historical_stddev_col: str = "moving_stddev",
    z_threshold: float = 3.5
) -> DataFrame:
    """
    Evaluates point-in-time metrics against window baselines using Z-Score scoring.
    Z = (Value - Mean) / StdDev
    
    Flags outliers where the absolute deviation exceeds the user-defined threshold.
    """
    # 1. Safely calculate absolute Z-score, avoiding divide-by-zero errors
    enriched_df = df.withColumn(
        "z_score",
        F.when(
            F.col(historical_stddev_col) == 0, 0.0
        ).otherwise(
            F.abs((F.col("metric_value") - F.col(historical_mean_col)) / F.col(historical_stddev_col))
        )
    )
    
    # 2. Flag anomalies breaking validation criteria
    flagged_df = enriched_df.withColumn(
        "is_z_score_outlier",
        F.when(F.col("z_score") > z_threshold, 1).otherwise(0)
    )
    
    return flagged_df
