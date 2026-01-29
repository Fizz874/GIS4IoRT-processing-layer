import rclpy
from rclpy.node import Node
from sensor_msgs.msg import NavSatFix
import paho.mqtt.client as mqtt
import sys, json, time
from data_logger import BufferedLogger

class Ros2MqttBridge(Node):
    def __init__(self, mqtt_client, robot_name, ros2_topic, mqtt_topic, logger):
        super().__init__('ros2_mqtt_bridge')
        self.mqtt_client = mqtt_client
        self.robot_name = robot_name
        self.ros2_topic = ros2_topic
        self.mqtt_topic = mqtt_topic
        self.logger = logger
        self.create_subscription(
            NavSatFix, 
            self.ros2_topic,
            self.callback, 
            10)
        self.get_logger().info(f"Bridge started for {self.robot_name}")
    def callback(self, msg):
        payload = {
            "timestamp": int(time.time() * 1000),
            "robot_name": self.robot_name,
            "position_x": msg.longitude,
            "position_y": msg.latitude
        }
        self.logger.log(payload)
        try:
            self.mqtt_client.publish(self.mqtt_topic, json.dumps(payload))
        except Exception as e:
            self.get_logger().error(f"MQTT publish failed: {e}")

def main():
    if len(sys.argv) < 6:
        print("Usage: python3 script.py <IP> <PORT> <ROBOT_NAME> <ROS2_TOPIC> <MQTT_TOPIC>")
        return
    m_ip, m_port, r_name, r_topic, m_topic = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4], sys.argv[5]
    start_ts = int(time.time() * 1000)
    log_file = f"/logs/{r_name}_{start_ts}.csv"
    fields = ["timestamp", "robot_name", "position_x", "position_y"]
    csv_logger = BufferedLogger(log_file, fields, buffer_size=100, flush_interval=10)
    client = mqtt.Client(
        client_id=f"{r_name}_mqtt_bridge",
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        transport="websockets",
        protocol=mqtt.MQTTv5
    )
    node = None
    try:
        client.connect(m_ip, m_port, 60)
        client.loop_start()
        rclpy.init()
        node = Ros2MqttBridge(client, r_name, r_topic, m_topic, csv_logger)
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"Critical Error: {e}")
    finally:
        print("Shutting down bridge...", flush=True)
        csv_logger.close()
        if node:
            node.destroy_node()
        rclpy.shutdown()
        client.loop_stop()
        client.disconnect()

if __name__ == '__main__':
    main()
