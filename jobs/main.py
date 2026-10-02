#!/usr/bin/env python3
"""
SentinelStream Entrypoint.
Parses environment parameters and launches the streaming engine.
"""

import argparse
import sys
import logging

# Configure logger to output production-standard formatted logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger("sentinel-stream-main")

# Import the engine launcher (to be created in jobs/streaming_engine.py)
from streaming_engine import run_pipeline

def parse_arguments():
    """Parses application configurations passed via spark-submit command line."""
    parser = argparse.ArgumentParser(description="SentinelStream Real-Time Analytics Job")
    
    parser.add_argument(
        "--kafka-brokers",
        required=True,
        help="Comma-separated list of Kafka broker addresses (e.g., localhost:9092)"
    )
    parser.add_argument(
        "--input-topic",
        default="iot-sensor-payloads",
        help="Kafka topic to ingest raw streaming telemetry from"
    )
    parser.add_argument(
        "--clean-output-topic",
        default="sensors-clean-metrics",
        help="Kafka destination topic for successfully processed and enriched payloads"
    )
    parser.add_argument(
        "--dlq-topic",
        default="sensors-dead-letter",
        help="Kafka topic used as a Dead Letter Queue (DLQ) for corrupt/invalid schemas"
    )
    parser.add_argument(
        "--checkpoint-dir",
        default="/tmp/spark-checkpoints/sentinel_stream",
        help="Storage path for PySpark structured streaming exactly-once checkpoints"
    )
    
    return parser.parse_args()

def main():
    """Application orchestration flow."""
    logger.info("Initializing SentinelStream pipeline execution environment...")
    
    args = parse_arguments()
    
    # Log active deployment parameters (excluding sensitive details if any)
    logger.info(f"Targeting Kafka Brokers: {args.kafka_brokers}")
    logger.info(f"Ingesting from topic: {args.input_topic}")
    logger.info(f"Routing validated metrics to: {args.clean_output_topic}")
    logger.info(f"Routing anomalies/errors to DLQ: {args.dlq_topic}")

    try:
        # Hand off execution control to the streaming engine engine
        run_pipeline(
            brokers=args.kafka_brokers,
            input_topic=args.input_topic,
            output_topic=args.clean_output_topic,
            dlq_topic=args.dlq_topic,
            checkpoint_dir=args.checkpoint_dir
        )
    except KeyboardInterrupt:
        logger.info("Graceful shutdown sequence triggered by user interrupt signal.")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"Fatal pipeline termination crash due to unhandled error: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
