import os
import time
import json
import threading
import subprocess
import sys
import random
from datetime import datetime
from kafka import KafkaConsumer

import ksqldb_managerKSQLDB as ksqldb_manager

ITERATIONS = 9
OUTPUT_DIR = "test_results"
KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC_ROBOTS = "ros_gps_fix"

# Docker
DOCKER_CONTAINER = "ros2_bridge"
SENSOR_GENERATOR_SCRIPT = "scripts/kafka_sensor_producerKSQLDB.py"

current_sensor_process = None
current_collector = None
current_input_logger = None

# JSON Deserializer
def safe_json_deserializer(x):
    try:
        if x is None: return None
        text = x.decode('utf-8').strip()
        if not text: return None
        return json.loads(text)
    except Exception:
        return None

# Robot Input Logger
class RobotInputLogger(threading.Thread):
    """
    Listens to the INPUT topic (ros_gps_fix) and logs raw robot data 
    to robot_raw.jsonl for verification.
    """
    def __init__(self, save_path): 
        super().__init__()
        self.save_path = save_path
        self.running = True
        self.daemon = True 
        
        # Determine Group ID
        self.group_id = f"logger_robot_{random.randint(10000, 99999)}"

    def run(self):
        try:
            # Open file in append mode with line buffering
            with open(self.save_path, 'a', encoding='utf-8') as f:
                
                consumer = KafkaConsumer(
                    INPUT_TOPIC_ROBOTS,          
                    bootstrap_servers=KAFKA_BROKER,
                    auto_offset_reset='latest',
                    value_deserializer=safe_json_deserializer,
                    key_deserializer=lambda k: k.decode('utf-8') if k else None,
                    group_id=self.group_id,
                    consumer_timeout_ms=1000 
                )
                
                print(f"      [LOGGER] Recording raw robot data to {os.path.basename(self.save_path)}")
                
                while self.running:
                    msg_pack = consumer.poll(timeout_ms=500)
                    for tp, messages in msg_pack.items():
                        for msg in messages:
                            val = msg.value
                            key = msg.key
                            if val:
                                log_entry = {
                                    "id": key if key else "unknown",
                                    "ts": val.get('timestamp'),
                                    "lat": val.get('latitude'),
                                    "lon": val.get('longitude')
                                }
                                f.write(json.dumps(log_entry) + "\n")
                                f.flush()
                
                consumer.close()
        except Exception as e:
            print(f"      [ERROR] RobotInputLogger failed: {e}")

    def stop(self):
        self.running = False
        if self.is_alive():
            self.join(timeout=1.0)


# Output Data Collector
class DataCollector(threading.Thread):
    def __init__(self, test_id, target_topic, save_dir): 
        super().__init__()
        self.test_id = test_id
        self.target_topic = target_topic     
        self.save_dir = save_dir
        self.running = True
        self.collected_data = []
        self.group_id = f"collector_{test_id}_{random.randint(10000, 99999)}"
        self.daemon = True 

    def run(self):
        try:
            print(f"      [KAFKA] Listening on topic: {self.target_topic}")
            consumer = KafkaConsumer(
                self.target_topic,          
                bootstrap_servers=KAFKA_BROKER,
                auto_offset_reset='latest',
                value_deserializer=safe_json_deserializer,
                group_id=self.group_id,
                consumer_timeout_ms=1000 
            )
            print(f"      [KAFKA] Collector connected (Group: {self.group_id})")
            
            while self.running:
                msg_pack = consumer.poll(timeout_ms=500)
                for tp, messages in msg_pack.items():
                    for msg in messages:
                        record = msg.value
                        if record:
                            record['ts_received'] = int(time.time() * 1000)
                            self.collected_data.append(record)
            
            consumer.close()
            
        except Exception as e:
            print(f"      [ERROR] Collector error: {e}")

    def stop_and_save(self):
        self.running = False
        if self.is_alive():
            self.join(timeout=2.0) 
        
        filename = os.path.join(self.save_dir, f"results_{self.test_id}.json")        

        if not self.collected_data:
            print(f"   [IO] No data to save for {self.test_id} (Test aborted?)")
            return

        report = {
            "test_id": self.test_id,
            "topic": self.target_topic, 
            "timestamp": datetime.now().isoformat(),
            "records": len(self.collected_data),
            "data": self.collected_data
        }
        
        try:
            with open(filename, 'w') as f:
                json.dump(report, f, indent=2)
            print(f"   [IO] Saved: {filename} (Records: {len(self.collected_data)})")
        except Exception as e:
            print(f"   [IO] File write error: {e}")


def cleanup():
    """Cleanup function called on error or Ctrl+C"""
    global current_sensor_process, current_collector, current_input_logger
    
    print("\n[CLEANUP] Cleaning up processes...")
    
    # Stop Sensor Generator
    if current_sensor_process:
        if current_sensor_process.poll() is None:
            print("   -> Sending SIGINT to generator...")
            current_sensor_process.terminate() 
            try:
                current_sensor_process.wait(timeout=5) 
            except subprocess.TimeoutExpired:
                current_sensor_process.kill()

    # Stop Output Collector
    if current_collector and current_collector.is_alive():
        print("   -> Stopping Output Collector...")
        current_collector.running = False
        current_collector.stop_and_save()

    # Stop Input Logger
    if current_input_logger and current_input_logger.is_alive():
        print("   -> Stopping Input Robot Logger...")
        current_input_logger.stop()

def run_tests():
    global current_sensor_process, current_collector, current_input_logger

    if not os.path.exists(OUTPUT_DIR): 
        os.makedirs(OUTPUT_DIR)

    print(f"=== STARTING TESTS (Ctrl+C to abort all) ===")

    for i in range(1, ITERATIONS + 1):
    #for i in [1,2,3]:#1,2,3,4,5,6,7,8,9
        meta = ksqldb_manager.get_test_metadata(i)
        
        if not meta:
            print(f"[SKIP] No configuration for iteration {i}")
            continue

        test_id = meta['config_name'] 
        dynamic_topic = meta['output_topic'] 
        dynamic_ros_cmd = meta['ros_cmd']    

        test_run_dir = os.path.join(OUTPUT_DIR, test_id)
        if not os.path.exists(test_run_dir):
            os.makedirs(test_run_dir)

        # Paths for logs
        robot_log_path = os.path.join(test_run_dir, "robot_raw.jsonl")
        sensor_log_path = os.path.join(test_run_dir, "sensor_out.jsonl")

        print(f"\n--- RUN {i}/{ITERATIONS} [{test_id}] ---")
        
        current_sensor_process = None
        current_collector = None
        current_input_logger = None
        
        try:
            # Deploy KSQLDB Config
            try:
                deployed_name = ksqldb_manager.deploy_configuration(i)
                if not deployed_name:
                    raise RuntimeError("Deployment returned empty name")
            except Exception as e:
                print(f"   [CRITICAL] Configuration error: {e}")
                cleanup()
                continue

            # Start Data Collector (Output)
            print(f"   [1/4] Starting Data Collectors...")
            current_collector = DataCollector(test_id, dynamic_topic, test_run_dir)
            current_collector.start()
            
            # Start Input Logger
            current_input_logger = RobotInputLogger(robot_log_path)
            current_input_logger.start()

            time.sleep(2.0)

            # Start Sensor Generator (for HUMIDITY 4-6)
            config_type = ksqldb_manager.TEST_CONFIGS_MAP.get(i, {}).get('type')
            if config_type == 'HUMIDITY':
                print(f"   [2/4] Starting Sensor Generator...")
                current_sensor_process = subprocess.Popen([
                    sys.executable, 
                    SENSOR_GENERATOR_SCRIPT,
                    "--robot_log", robot_log_path,
                    "--sensor_log", sensor_log_path
                ])
                time.sleep(2.0)
            else:
                print(f"   [2/4] Skipping Sensor Generator (not humidity test)...")
            
            # Start Robot (ROS Bag)
            print(f"   [3/4] Starting Robot...")
            subprocess.run([
                "docker", "exec", DOCKER_CONTAINER, 
                "bash", "-c", dynamic_ros_cmd  
            ], check=True, stdout=subprocess.DEVNULL)
            
            print("         -> Robot finished route. Waiting for data ingestion...")
            time.sleep(5.0)

            cleanup()
            
        except subprocess.CalledProcessError as e:
            print(f"   [ERROR] Subprocess command error: {e}")
            cleanup()
        
        finally:
            print(f"   [CLEANUP] Cleaning up Job after iteration {i}...")
            ksqldb_manager.cleanup_job(i)
            cleanup()
            print(f"[WAIT]")
            time.sleep(30.0)

    print("\n=== TESTS FINISHED ===")

if __name__ == "__main__":
    try:
        run_tests()
    except KeyboardInterrupt:
        print("\n\n!!! CTRL+C DETECTED - EMERGENCY STOP !!!")
        cleanup()
        sys.exit(0)