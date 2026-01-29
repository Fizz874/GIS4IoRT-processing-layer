from fastapi import APIRouter, HTTPException, Depends
from app.adapters.ksqldb.schemas import GeofenceRequest
from app.adapters.ksqldb import database
from app.adapters.ksqldb.kafka_service import KafkaService, get_kafka_service
import logging

router = APIRouter()
logger = logging.getLogger("uvicorn.info")

@router.post("/geofence", tags=["Control"])  
async def add_geofence_rule(
    data: GeofenceRequest,
    kafka: KafkaService = Depends(get_kafka_service)
):
    # Validation
    robot_exists = database.get_robot(data.robot_id)
    if not robot_exists:
         raise HTTPException(404, detail=f"Robot '{data.robot_id}' does not exist. Register it first via POST /robots.")

    zone = database.get_zone(data.zone_id)
    if not zone:
        raise HTTPException(404, detail=f"Zone '{data.zone_id}' not found via API.")

    # Saving to the database
    database.add_geofence_assignment(data.robot_id, data.zone_id, data.config_name)
    
    # Get all robots assignments
    all_assignments = database.get_all_assignments_for_robot(data.robot_id)
    
    # Build strings
    hex_list = [item['geo'] for item in all_assignments]
    composite_hex = "|".join(hex_list)

    config_list = list(set([item['config_name'] for item in all_assignments]))
    composite_configs = "|".join(config_list)
    
    # Sending
    CONTROL_TOPIC = "geofence_control"
    
    if data.robot_id:
        await kafka.send_robot_allow(data.robot_id, CONTROL_TOPIC, composite_hex, composite_configs)
    return {
        "status": "assigned", 
        "robot_id": data.robot_id,
        "active_zones_count": len(hex_list),
        "active_configs": composite_configs
    }


# Remove geofence
@router.delete("/geofence", tags=["Control"])
async def remove_geofence_rule(
    data: GeofenceRequest,
    kafka: KafkaService = Depends(get_kafka_service)
):
    # Delete specific assignment
    database.remove_geofence_assignment(data.robot_id, data.config_name)
    
    CONTROL_TOPIC = "geofence_control"
    
    # Check whats left in the database
    remaining_assignments = database.get_all_assignments_for_robot(data.robot_id)
    
    if not remaining_assignments:
        # If nothing is left send OFF
        logger.info(f"No geofence rules left for {data.robot_id}. Sending OFF.")
        await kafka.send_robot_block(data.robot_id, CONTROL_TOPIC)
        current_status = "OFF"
    else:
        # If something is left recalculate HEXs, configs and send UPDATE
        logger.info(f"Updating geofence rules for {data.robot_id}...")
        
        # Geometry (HEXs joined with pipe)
        hex_list = [item['geo'] for item in remaining_assignments]
        composite_hex = "|".join(hex_list)

        # Config names for the frontend
        config_list = list(set([item['config_name'] for item in remaining_assignments]))
        composite_configs = "|".join(config_list)
        
        # Send new state with ON flag
        await kafka.send_robot_allow(data.robot_id, CONTROL_TOPIC, composite_hex, composite_configs)
        current_status = f"ON (updated: {composite_configs})"

    return {
        "status": "removed", 
        "robot_id": data.robot_id,
        "kafka_status": current_status
    }

@router.get("/geofence", tags=["Control"])
def list_geofence_rules():
    return database.get_all_assignments()

# Hard Reset
@router.delete("/geofence/reset/{robot_id}", tags=["Control"])
async def reset_geofence(
    robot_id: str, 
    kafka: KafkaService = Depends(get_kafka_service)
):
    try:
        database.remove_all_geofence_assignments_for_robot(robot_id)
    except AttributeError:
        raise HTTPException(500, detail="Method remove_all_geofence_assignments_for_robot missing in database.py")
    except Exception as e:
        logger.error(f"DB Error: {e}")
        raise HTTPException(500, detail=str(e))
    
    CONTROL_TOPIC = "geofence_control"
    await kafka.send_robot_block(robot_id, CONTROL_TOPIC)
    
    logger.warning(f"NUCLEAR RESET performed for robot: {robot_id} (Geofence)")
    return {"status": "reset_complete", "robot_id": robot_id}