import paho.mqtt.client as mqtt
import sys
import json
import time
from data_logger import BufferedLogger

BROKER_IP = sys.argv[1]
PORT = int(sys.argv[2])
TOPICS = sys.argv[3].split(';') 

start_ts = int(time.time() * 1000)
log_file = f"/logs/subscriber_{start_ts}.csv"
fields = ["arrival_timestamp", "topic", "payload"]
csv_logger = BufferedLogger(log_file, fields, buffer_size=50, flush_interval=10)
def on_connect(client, userdata, flags, reasonCode, properties):
    for topic in TOPICS:
        client.subscribe(topic)
        print(f"Subscribed to {topic}", flush=True)
def on_message(client, userdata, msg):
    arrival_ts = int(time.time() * 1000)
    try:
        payload_str = msg.payload.decode()
        log_entry = {
            "arrival_timestamp": arrival_ts,
            "topic": msg.topic,
            "payload": payload_str
        }
        csv_logger.log(log_entry)
        data = json.loads(payload_str)
        output_parts = [f'{key}: {value}' for key, value in data.items()]
        output_string = f"[{msg.topic}] " + ", ".join(output_parts)
        print(output_string, flush=True)
    except Exception as e:
        print(f"Error: {e}", flush=True)
client = mqtt.Client(
    client_id="query_listener",
    transport="websockets",
    protocol=mqtt.MQTTv5,
    callback_api_version=mqtt.CallbackAPIVersion.VERSION2
)
client.on_connect = on_connect
client.on_message = on_message
client.connect(BROKER_IP, PORT, 60)

try:
    print(f"Subscriber started. Logging to {log_file}", flush=True)
    client.loop_forever()
except KeyboardInterrupt:
    pass
except Exception as e:
    print(f"Error: {e}")
finally:
    csv_logger.close()
    client.disconnect()
