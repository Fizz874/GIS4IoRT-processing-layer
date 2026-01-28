import rclpy
import time 
from rclpy.node import Node
from sensor_msgs.msg import NavSatFix
from kafka import KafkaProducer
import json
from functools import partial 
import math

class MultiRobotKafkaBridge(Node):
    def __init__(self):
        super().__init__('multi_robot_kafka_bridge')

        self.declare_parameter('robot_list', ['leader', 'follower'])
        self.declare_parameter('kafka_bootstrap_servers', 'broker:29092')
        self.declare_parameter('kafka_topic', 'ros_gps_fix')

        self.robot_list = self.get_parameter('robot_list').get_parameter_value().string_array_value
        self.kafka_servers = self.get_parameter('kafka_bootstrap_servers').get_parameter_value().string_value
        self.target_kafka_topic = self.get_parameter('kafka_topic').get_parameter_value().string_value

        self.get_logger().info(f'Connecting to Kafka: {self.kafka_servers}')
        self.get_logger().info(f'Target Kafka Topic: {self.target_kafka_topic}')

        self.producer = KafkaProducer(
            bootstrap_servers=[self.kafka_servers],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8') if x else None,
            acks=1, 
            retries=3
        )

        self.subscriptions_list = [] 
        
        for robot_name in self.robot_list:
            ros_topic = f'/{robot_name}/gps/fix'
            
            callback_with_name = partial(self.gps_callback, robot_name=robot_name)
            
            sub = self.create_subscription(
                NavSatFix,
                ros_topic,
                callback_with_name,
                10
            )
            self.subscriptions_list.append(sub)
            self.get_logger().info(f'Subscribed to: {ros_topic} for robot: {robot_name}')

    def gps_callback(self, msg, robot_name):
        # Map robot names to unit1/unit2 format
        robot_name = {'leader': 'unit1', 'follower': 'unit2'}.get(robot_name, robot_name)
        
        if math.isnan(msg.latitude) or math.isnan(msg.longitude):
            self.get_logger().warn(f"Skipping NaN GPS from {robot_name}")
            return

        # Use current time for timestamp
        timestamp_ms = int(time.time() * 1000)

        payload = {
            'timestamp': timestamp_ms,
            'latitude': msg.latitude,
            'longitude': msg.longitude,
            'altitude': msg.altitude
        }

        try:
            self.producer.send(
                self.target_kafka_topic, 
                value=payload, 
                key=robot_name
            )
            
        except Exception as e:
            self.get_logger().error(f'Failed to send to Kafka: {e}')


def main(args=None):
    rclpy.init(args=args)
    node = MultiRobotKafkaBridge()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        node.get_logger().info("Stopped by the user (Ctrl+C)")
    except rclpy.executors.ExternalShutdownException:
        node.get_logger().info("ROS2 context closed (SIGTERM)")
    finally:
        node.producer.flush()
        node.destroy_node()
        if rclpy.ok():
            rclpy.shutdown()

if __name__ == '__main__':
    main()