#!/usr/bin/env python3

import rclpy
from rclpy.node import Node
from nav_msgs.msg import Odometry
from sensor_msgs.msg import NavSatFix
from kafka import KafkaProducer
import json
import os
import sys

# Kafka Configuration
KAFKA_ODOM_TOPIC = 'ros_filtered_odom'
KAFKA_GPS_TOPIC = 'ros_gps_fix'
KAFKA_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'broker:29092')

class ROS2KafkaBridge(Node):
    def __init__(self):
        super().__init__('ros2_kafka_bridge')
        self.producer = None

        self.get_logger().info("--- Initializing ROS2-Kafka Bridge ---")
        
        # Connect to Kafka
        try:
            self.get_logger().info(f"Connecting to Kafka at {KAFKA_SERVERS}...")
            self.producer = KafkaProducer(
                bootstrap_servers=[KAFKA_SERVERS],
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                key_serializer=lambda x: str(x).encode('utf-8'),
                acks='all'
            )
            self.get_logger().info("Successfully connected to Kafka")
        except Exception as e:
            self.get_logger().error(f"Failed to connect to Kafka: {e}")
            sys.exit(1) 

        # Odometry Subscriptions
        self.create_subscription(
            Odometry,
            '/leader/localisation/filtered_odom',
            lambda msg: self.odom_callback(msg, 'leader'),
            10)
        
        self.create_subscription(
            Odometry,
            '/follower/localisation/filtered_odom',
            lambda msg: self.odom_callback(msg, 'follower'),
            10)
        
        # GPS Subscriptions
        self.create_subscription(
            NavSatFix,
            '/leader/gps/fix',
            lambda msg: self.gps_callback(msg, 'leader'),
            10)
        
        self.create_subscription(
            NavSatFix,
            '/follower/gps/fix',
            lambda msg: self.gps_callback(msg, 'follower'),
            10)

        self.get_logger().info("Bridge is live. Waiting for ROS2 messages...")
        self.message_count = {'leader': 0, 'follower': 0, 'leader_gps': 0, 'follower_gps': 0}

    # Receives Odometry and publishes to Kafka
    def odom_callback(self, msg, robot_id):
        try:
            timestamp_ms = (msg.header.stamp.sec * 1000) + (msg.header.stamp.nanosec // 1000000)
            #print(str(msg.header.stamp.sec) + "sec " + str(msg.header.stamp.nanosec) + "nanosec")
            data = {
                'timestamp': timestamp_ms,
                'position_x': msg.pose.pose.position.x,
                'position_y': msg.pose.pose.position.y,
                'position_z': msg.pose.pose.position.z,
                'source_topic': f"/{robot_id}/localisation/filtered_odom"
            }
            #print(data)
            self.producer.send(KAFKA_ODOM_TOPIC, value=data, key=robot_id)

            self.message_count[robot_id] += 1
            if self.message_count[robot_id] % 100 == 0:
                self.get_logger().info(f"Published odom message #{self.message_count[robot_id]} for '{robot_id}'")

        except Exception as e:
            self.get_logger().error(f"Error processing odom message for {robot_id}: {e}")

    # Receives NavSatFix and publishes to Kafka
    def gps_callback(self, msg, robot_id):
        try:
            timestamp_ms = (msg.header.stamp.sec * 1000) + (msg.header.stamp.nanosec // 1000000)
            
            data = {
                'timestamp': timestamp_ms,
                'latitude': msg.latitude,
                'longitude': msg.longitude,
                'altitude': msg.altitude
            }
            print(robot_id, data)
            # Send to the new Kafka topic
            self.producer.send(KAFKA_GPS_TOPIC, value=data, key=robot_id)

            self.message_count[f"{robot_id}_gps"] += 1
            if self.message_count[f"{robot_id}_gps"] % 100 == 0:
                self.get_logger().info(f"Published GPS message #{self.message_count[f'{robot_id}_gps']} for '{robot_id}'")

        except Exception as e:
            self.get_logger().error(f"Error processing GPS message for {robot_id}: {e}")

    # Close the producer
    def shutdown(self):
        if self.producer:
            self.get_logger().info("Flushing and closing Kafka producer...")
            self.producer.flush()
            self.producer.close()
            self.get_logger().info("Producer closed")

def main(args=None):
    rclpy.init(args=args)
    bridge_node = ROS2KafkaBridge()
    try:
        rclpy.spin(bridge_node)
    except KeyboardInterrupt:
        pass
    finally:
        bridge_node.shutdown()
        bridge_node.destroy_node()
        rclpy.shutdown()

if __name__ == '__main__':
    main()