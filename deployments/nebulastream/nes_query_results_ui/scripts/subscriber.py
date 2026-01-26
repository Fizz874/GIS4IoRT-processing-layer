import paho.mqtt.client as mqtt
import sys
import json

BROKER_IP = sys.argv[1]
PORT = int(sys.argv[2])
TOPICS = sys.argv[3].split(';') 

def on_connect(client, userdata, flags, reasonCode, properties):
    for topic in TOPICS:
        client.subscribe(topic)

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
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
    client.loop_forever()
except KeyboardInterrupt:
    pass
finally:
    client.disconnect()
