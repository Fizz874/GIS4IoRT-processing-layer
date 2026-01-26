import sys
import time
import json
import random
import csv
import paho.mqtt.client as mqtt

class Sensor:
    def __init__(self, sensor_id, lon, lat, mode):
        self.sensor_id = sensor_id
        self.lon = float(lon)
        self.lat = float(lat)
        self.mode = mode.strip().upper()
        self.next_emit_time = time.time() + random.uniform(0, 1.0)
    def get_humidity(self, current_time):
        if self.mode == 'HIGH':
            return 90.0
        elif self.mode == 'LOW':
            return 70.0
        elif self.mode == 'SWITCH':
            cycle_position = current_time % 20
            if cycle_position < 10:
                return 70.0
            else:
                return 90.0
        else:
            return 0.0
    def should_emit(self, current_time):
        return current_time >= self.next_emit_time
    def schedule_next(self, current_time):
        jitter = random.uniform(-0.01, 0.01)
        self.next_emit_time = current_time + 1.0 + jitter
    def get_payload(self, current_time):
        humidity = self.get_humidity(current_time)
        return {
            "timestamp": int(current_time * 1000),
            "sensor_id": self.sensor_id,
            "position_x": self.lon,
            "position_y": self.lat,
            "humidity": round(humidity, 1)
        }

def load_sensors(file_path):
    sensors = []
    try:
        with open(file_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                s = Sensor(
                    row['id'],
                    row['lon'],
                    row['lat'],
                    row['mode']
                )
                sensors.append(s)
        return sensors
    except Exception as e:
        print(f"Error {e}")
        sys.exit(1)

def main():
    if len(sys.argv) < 4:
        print("Usage: python3 humidity_producer.py <MQTT_IP> <MQTT_PORT> <SENSORS_CSV>")
        sys.exit(1)
    mqtt_ip = sys.argv[1]
    mqtt_port = int(sys.argv[2])
    sensors_file = sys.argv[3]
    mqtt_topic = "humidity_producer"
    sensors = load_sensors(sensors_file)
    client = mqtt.Client(
        client_id="humidity_producer",
        transport="websockets",
        protocol=mqtt.MQTTv5,
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2
    )
    try:
        client.connect(mqtt_ip, mqtt_port, 60)
        client.loop_start()
        while True:
            now = time.time()
            for sensor in sensors:
                if sensor.should_emit(now):
                    payload = sensor.get_payload(now)
                    try:
                        client.publish(mqtt_topic, json.dumps(payload))
                    except Exception as e:
                        print(f"Error {e}")
                    sensor.schedule_next(now)
            time.sleep(0.01)
    except KeyboardInterrupt:
        print("Stopping producer...")
    except Exception as e:
        print(f"Error {e}")
    finally:
        client.loop_stop()
        client.disconnect()

if __name__ == '__main__':
    main()
