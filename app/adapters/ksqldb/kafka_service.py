import logging
import asyncio
import json
from aiokafka import AIOKafkaProducer
from app.config import settings

logger = logging.getLogger("uvicorn.error")

class KafkaService:
    def __init__(self):
        self.producer = None
        self.broker_url = settings.KAFKA_BOOTSTRAP_SERVERS

    async def start(self):
        logger.info(f"Connecting to Kafka Producer at {self.broker_url}...")
        try:
            self.producer = AIOKafkaProducer(bootstrap_servers=self.broker_url)
            await self.producer.start()
            logger.info("Kafka Producer connected successfully")
        except Exception as e:
            logger.error(f"Failed to connect Kafka Producer: {e}")

    async def stop(self):
        if self.producer:
            await self.producer.stop()

    async def _send_json(self, topic: str, key: str, value: dict, timestamp_ms: int = None):
        """
        Sends JSON to Kafka
        Args:
            timestamp_ms: If None, Kafka will use current time / If 0 or different time then it will be used
        """
        if not self.producer:
            logger.warning("Producer not connected")
            return
        try:
            payload = json.dumps(value).encode('utf-8')
            k = str(key).encode('utf-8')
            
            send_kwargs = {
                "topic": topic,
                "key": k,
                "value": payload
            }
            
            if timestamp_ms is not None:
                send_kwargs["timestamp_ms"] = timestamp_ms
            
            await self.producer.send_and_wait(**send_kwargs)
            
            log_ts = timestamp_ms if timestamp_ms is not None else "auto"
            logger.info(f"Sent to {topic} [{key}] ts={log_ts}: {value}")
            
        except Exception as e:
            logger.error(f"Kafka send error: {e}")


    async def register_robot(self, robot_id: str):
        """
        Registers robot in ksqlDB to allow its telemetry through
        Topic: robot_registration
        """
        payload = {"status": "REGISTERED"}
        await self._send_json("robot_registration", str(robot_id), payload)

    async def deregister_robot(self, robot_id: str):
        """
        Blocks robot telemetry
        """
        payload = {"status": "DELETED"}
        await self._send_json("robot_registration", str(robot_id), payload)

    # REGISTRY: SENSORS 
    async def register_sensor(self, sensor_id: str):
        """
        Registers sensor in ksqlDB to allow its data through
        Topic: sensor_registration
        """
        payload = {"status": "REGISTERED"}
        await self._send_json("sensor_registration", str(sensor_id), payload)

    async def deregister_sensor(self, sensor_id: str):
        """
        Blocks sensor data
        """
        payload = {"status": "DELETED"}
        await self._send_json("sensor_registration", str(sensor_id), payload)


    async def send_robot_allow(self, robot_id: str, topic: str, poly_hex: str, config_names: str):
        """
        Sends geofence configuration for a robot
        Using None so that Kafka inserts current time
        """
        payload = {
            "robot_id": robot_id,
            "status": "ON",
            "poly_hex": poly_hex,
            "config_names": config_names
        }
        await self._send_json(topic, robot_id, payload, timestamp_ms=None)
        
    async def send_robot_block(self, robot_id: str, topic: str):
        """
        Turns off geofencing for a robot
        Sends status=OFF message that will overwrite the previous configuration
        """
        await self._send_json(topic, robot_id, {"status": "OFF"}, timestamp_ms=None)

    async def send_collision_control_on(self, config_name: str):
        """Activates collision monitoring"""
        payload = {
            "status": "ON",
            "config_name": config_name
        }
        await self._send_json("collision_control", "global", payload)

    async def send_collision_control_off(self):
        """Deactivates collision monitoring"""
        payload = {
            "status": "OFF",
            "config_name": "none"
        }
        await self._send_json("collision_control", "global", payload)



    # HUMIDITY RULES 
    async def send_humidity_rule(self, sensor_id: str, min_humidity: float, radius_m: float):
        """
        Sends a single rule to ksqlDB
        Topic: humidity_control
        Key: sensor_id
        """
        payload = {
            "sensor_id": str(sensor_id),
            "min_humidity": min_humidity,
            "radius_m": radius_m,
            "status": "ON"
        }
        await self._send_json("humidity_control", str(sensor_id), payload)

    async def send_humidity_rule_off(self, sensor_id: str):
        """
        Disables the rule for this sensor
        """
        payload = {
            "sensor_id": str(sensor_id),
            "min_humidity": 0.0,
            "radius_m": 0.0,
            "status": "OFF"
        }
        await self._send_json("humidity_control", str(sensor_id), payload)

kafka_service = KafkaService()
def get_kafka_service(): 
    return kafka_service