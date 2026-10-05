import json
import time
import math
import random
from datetime import datetime, timezone
from kafka import KafkaProducer

# ---------------------------------------------------------------------------
# CONFIGURATION & PARAMETERS
# ---------------------------------------------------------------------------
BOOTSTRAP_SERVERS = ['localhost:9094']
TOPIC_NAME = 'iot-sensor-stream'
DEVICE_IDS = ['sensor-device-01', 'sensor-device-02', 'sensor-device-03']

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    key_serializer=lambda k: str(k).encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"📡 SentinelStream Data Generator started. Streaming to topic: {TOPIC_NAME}...")

# ---------------------------------------------------------------------------
# TELEMETRY GENERATION ENGINE
# ---------------------------------------------------------------------------
step = 0

try:
    while True:
        current_time = datetime.now(timezone.utc).isoformat()
        
        for device in DEVICE_IDS:
            # 1. Generate normal base-state metric using a smooth sine wave (e.g., thermal cycle)
            base_temp = 25.0 + 5.0 * math.sin(step * 0.1)
            # Add random micro-noise (Gaussian distribution)
            normal_noise = random.gauss(0, 0.5)
            metric_value = base_temp + normal_noise
            
            anomaly_triggered = "NONE"
            
            # 2. INTENTIONAL ANOMALY INJECTION (Simulated every ~100 steps per device)
            # This directly provides verification validation testing for your PySpark logic
            if step > 0 and step % 100 == 0:
                anomaly_selector = random.choice(["SPC", "Z_SCORE", "MULTI_DRIFT"])
                
                if anomaly_selector == "SPC":
                    # Task 1 Target: Gradual upward drift that will breach the Upper Control Limit (UCL)
                    metric_value += 12.0
                    anomaly_triggered = "SPC_3SIGMA_DRIFT"
                    
                elif anomaly_selector == "Z_SCORE":
                    # Task 2 Target: Extreme instantaneous massive spike (Outlier spike)
                    metric_value *= 5.0
                    anomaly_triggered = "Z_SCORE_SPIKE"
                    
                elif anomaly_selector == "MULTI_DRIFT":
                    # Task 3 Target: Covariance structural break (values stay in loose range but noise goes erratic)
                    metric_value = base_temp + random.uniform(8.0, 10.0)
                    anomaly_triggered = "ISOLATION_FOREST_CORRELATION_BREAK"

            # 3. Construct JSON Payload Matching PySpark Structured Schema
            payload = {
                "timestamp": current_time,
                "device_id": device,
                "metric_name": "temperature",
                "metric_value": round(metric_value, 4)
            }
            
            # Print feedback to console if an anomaly was injected
            if anomaly_triggered != "NONE":
                print(f"⚠️ [INJECTED {anomaly_triggered}] Device: {device} | Value: {payload['metric_value']}")
            
            # 4. Push event partition-keyed by device ID to Kafka
            producer.send(
                topic=TOPIC_NAME,
                key=device,
                value=payload
            )
            
        producer.flush()
        step += 1
        time.sleep(1.0) # Yields 1 record group per second

except KeyboardInterrupt:
    print("\n🛑 Ingestion streaming stopped by user.")
finally:
    producer.close()
