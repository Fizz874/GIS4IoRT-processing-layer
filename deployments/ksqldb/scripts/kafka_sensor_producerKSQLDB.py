import json
import time
import sys
import os
import signal
import argparse
from decimal import Decimal
from kafka import KafkaProducer

# --- CONFIGURATION ---
KAFKA_BROKER = "localhost:9092"
TOPIC_OUT_SENSOR = "sensor_proximity"
CONFIG_FILE = "./scripts/sensor_config_updated.json"

# VALUES
VAL_LOW = 35.0
VAL_HIGH = 90.0
INTERVAL_SECONDS = 10

class DeterministicSensor:
    def __init__(self, sensor_log_path):
        print(f"[INFO] Starting DETERMINISTIC Sensor Generator")
        print(f"[INFO] Pattern: 10s Heartbeat | Sensors 1-6 | Toggle Pattern")
        
        self.running = True
        signal.signal(signal.SIGINT, self.handle_exit)
        signal.signal(signal.SIGTERM, self.handle_exit)

        # Open log file
        self.sensor_log_file = open(sensor_log_path, "a", encoding="utf-8")

        # Kafka Producer
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        self.sensors = self.load_sensors()
        print(f"[INFO] Loaded {len(self.sensors)} sensors.")

    def handle_exit(self, signum, frame):
        print("\n[INFO] Shutting down...")
        self.running = False

    def load_sensors(self):
        if not os.path.exists(CONFIG_FILE):
            print(f"[ERROR] File missing: {CONFIG_FILE}")
            sys.exit(1)
            
        with open(CONFIG_FILE, 'r') as f:
            # We only need ID and Location (Lat/Lon)
            return json.load(f)

    def get_value_for_step(self, sensor_id, step_index):
        """
        Implements the pattern:
        Even Steps (t=0, 20, 40...): 1 0 1 0 1 1
        Odd Steps  (t=10, 30, 50...): 1 0 0 1 1 1
        """
        is_even = (step_index % 2 == 0)

        # S1: Always 1 (High)
        if sensor_id == 1: return VAL_HIGH
        
        # S2: Always 0 (Low)
        if sensor_id == 2: return VAL_LOW
        
        # S3: Toggles (1 -> 0)
        if sensor_id == 3: return VAL_HIGH if is_even else VAL_LOW
        
        # S4: Toggles (0 -> 1)
        if sensor_id == 4: return VAL_LOW if is_even else VAL_HIGH
        
        # S5: Always 1 (High)
        if sensor_id == 5: return VAL_HIGH
        
        # S6: Always 1 (High)
        if sensor_id == 6: return VAL_HIGH

        return VAL_LOW # Default

    def run(self):
        step_count = 0
        
        try:
            while self.running:
                current_ts = int(time.time() * 1000)
                print(f"\n--- T={step_count*10}s (Step {step_count}) ---")

                for s in self.sensors:
                    s_id = s['sensor_id']
                    
                    # 1. Calculate Value based on Pattern
                    humidity_val = self.get_value_for_step(s_id, step_count)
                    
                    # 2. Construct Payload
                    # ksqlDB expects: sensor_id, timestamp, position_x, position_y, humidity
                    payload = {
                        "sensor_id": str(s_id), # Explicitly requested in log
                        "timestamp": current_ts,
                        "position_x": float(s['lon']),
                        "position_y": float(s['lat']),
                        "humidity": humidity_val
                    }
                    
                    # 3. Send to Kafka
                    key = str(s_id).encode('utf-8')
                    # We send a copy without 'sensor_id' inside value if your schema doesn't support it, 
                    # BUT usually it's safer to include it. 
                    # If your ksqlDB stream is defined as:
                    # CREATE STREAM sensor_raw_stream (sensor_id VARCHAR KEY...
                    # Then it expects the ID in the key, but having it in value is fine too.
                    self.producer.send(TOPIC_OUT_SENSOR, value=payload, key=key)

                    # 4. Log to File (Flush immediately)
                    self.sensor_log_file.write(json.dumps(payload) + "\n")
                    self.sensor_log_file.flush()
                    
                    print(f"   [Sensor {s_id}] Sent: {humidity_val}")

                # Wait for next cycle
                time.sleep(INTERVAL_SECONDS)
                step_count += 1

        except Exception as e:
            print(f"[ERROR] {e}")
        finally:
            self.sensor_log_file.close()
            self.producer.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # Arguments kept for compatibility with the test runner, 
    # even though we don't use robot_log anymore.
    parser.add_argument("--robot_log", default="robots_raw.jsonl") 
    parser.add_argument("--sensor_log", default="sensors_out.jsonl")
    args = parser.parse_args()

    # We ignore robot_log input
    app = DeterministicSensor(args.sensor_log)
    app.run()