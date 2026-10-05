import pandas as pd
from typing import Iterator
from sklearn.ensemble import IsolationForest
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, DoubleType, LongType

def run_isolation_forest_microbatch(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    """
    Executes multi-variable Isolation Forest on incoming Arrow micro-batches.
    Scikit-Learn returns: -1 for anomalies, 1 for normal data.
    """
    for pdf in iterator:
        if pdf.empty:
            yield pdf
            continue

        # Extract features for multivariate analysis (e.g., metric_value and z_score)
        # You can add additional fields here (like device temperature vs. voltage)
        features = pdf[['metric_value', 'z_score']].fillna(0.0)

        # Initialize and fit the Isolation Forest model on the fly for this batch
        # contamination=0.05 targets flag rates around the top 5% anomalies
        model = IsolationForest(n_estimators=100, contamination=0.05, random_state=42)
        
        # Fit model and predict
        predictions = model.fit_predict(features)
        
        # Map output to standard binary flag: 1 for anomaly, 0 for normal
        pdf['is_iforest_anomaly'] = [1 if pred == -1 else 0 for pred in predictions]
        
        yield pdf

def evaluate_multivariate_anomalies(df: DataFrame) -> DataFrame:
    """
    Wraps mapInPandas execution to dynamically append Isolation Forest tracking schemas.
    """
    # Define exact output schema mapping back to Spark
    output_schema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("device_id", StringType(), True),
        StructField("metric_name", StringType(), True),
        StructField("metric_value", DoubleType(), True),
        StructField("moving_avg", DoubleType(), True),
        StructField("moving_stddev", DoubleType(), True),
        StructField("z_score", DoubleType(), True),
        StructField("is_spc_anomaly", LongType(), True),
        StructField("is_z_score_outlier", LongType(), True),
        StructField("is_iforest_anomaly", LongType(), True)
    ])

    # Select fields matching output layout precisely before calling mapInPandas
    ordered_df = df.select(
        "timestamp", "device_id", "metric_name", "metric_value", 
        "moving_avg", "moving_stddev", "z_score", "is_spc_anomaly", "is_z_score_outlier"
    )

    return ordered_df.mapInPandas(run_isolation_forest_microbatch, schema=output_schema)
