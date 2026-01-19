from fastapi import APIRouter, HTTPException, Depends
from app.adapters.ksqldb.schemas import SensorCreate, HumidityRuleRequest
from app.adapters.ksqldb import database
from app.adapters.ksqldb.kafka_service import KafkaService, get_kafka_service
import logging

router = APIRouter()
logger = logging.getLogger("uvicorn.info")

# SENSORS
@router.post("/sensors", tags=["Humidity Management"])
async def add_sensor(
    sensor: SensorCreate,
    kafka: KafkaService = Depends(get_kafka_service)
):
    database.upsert_sensor(
        database.SensorEntry(
            sensor.sensor_id
        )
    )
    
    await kafka.register_sensor(sensor.sensor_id)
    
    return {"status": "registered", "sensor_id": sensor.sensor_id}



@router.get("/sensors", tags=["Humidity Management"])
def list_sensors():
    return database.get_all_sensors()

@router.get("/sensors/{sensor_id}", tags=["Humidity Management"])
def get_sensor(sensor_id: str):
    sensor = database.get_sensor(sensor_id)
    if not sensor:
        raise HTTPException(404, detail=f"Sensor {sensor_id} not found")
    return sensor

@router.delete("/sensors/{sensor_id}", tags=["Humidity Management"])
async def delete_sensor(
    sensor_id: str,
    kafka: KafkaService = Depends(get_kafka_service)
):
    database.delete_sensor(sensor_id)
    
    await kafka.deregister_sensor(sensor_id)
    
    return {"status": "deleted", "sensor_id": sensor_id}


# RULES CONTROL
@router.post("/humidity-rule", tags=["Humidity Control"])
async def add_humidity_rule(
    data: HumidityRuleRequest,
    kafka: KafkaService = Depends(get_kafka_service)
):
    sensor = database.get_sensor(data.sensor_id)
    if not sensor:
        raise HTTPException(404, detail=f"Sensor {data.sensor_id} not found. Register it first via POST /sensors.")
    
    database.add_humidity_rule(
        data.sensor_id,
        data.min_humidity,
        data.alert_radius_m,
        data.config_name
    )
    
    await kafka.send_humidity_rule(
        sensor_id=data.sensor_id,
        min_humidity=data.min_humidity,
        radius_m=data.alert_radius_m
    )
    
    return {
        "status": "rule_activated",
        "sensor_id": data.sensor_id,
        "config_name": data.config_name,
        "threshold": data.min_humidity,
        "radius": data.alert_radius_m
    }

@router.delete("/humidity-rule", tags=["Humidity Control"])
async def remove_humidity_rule(
    data: HumidityRuleRequest,
    kafka: KafkaService = Depends(get_kafka_service)
):
    database.remove_humidity_rule(data.sensor_id, data.config_name)
    
    await kafka.send_humidity_rule_off(data.sensor_id)
    
    return {
        "status": "rule_deactivated",
        "sensor_id": data.sensor_id,
        "config_name": data.config_name
    }

@router.get("/humidity-rule", tags=["Humidity Control"])
def list_humidity_rules():
    return database.get_all_humidity_rules()