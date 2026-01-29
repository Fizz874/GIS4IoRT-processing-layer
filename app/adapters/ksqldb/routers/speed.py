from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import List, Optional
from app.adapters.ksqldb import database
from app.adapters.ksqldb.kafka_service import KafkaService, get_kafka_service
import logging

router = APIRouter()
logger = logging.getLogger("uvicorn.info")

# Pydantic Models
class SpeedRequest(BaseModel):
    robot_id: str
    config_name: str

class SpeedAssignment(BaseModel):
    id: int
    robot_id: str
    config_name: str
    status: str = "ACTIVE"

class SpeedListResponse(BaseModel):
    total_count: int
    assignments: List[SpeedAssignment]


@router.post("/speed", tags=["Control"], summary="Enable speed monitoring")
async def enable_speed_monitoring(
    data: SpeedRequest,
    kafka: KafkaService = Depends(get_kafka_service)
):
    # 1. Walidacja - czy robot istnieje
    robot_exists = database.get_robot(data.robot_id)
    if not robot_exists:
        raise HTTPException(404, detail=f"Robot '{data.robot_id}' does not exist.")
    
    # 2. Zapisz nowe przypisanie w bazie
    try:
        database.add_speed_assignment(data.robot_id, data.config_name)
    except Exception as e:
        logger.error(f"Database error: {e}")
        raise HTTPException(500, detail=f"Failed to save assignment: {str(e)}")
    
    # 3. Pobierz WSZYSTKIE przypisania i zbuduj string "speed1|speed2"
    try:
        all_assignments = database.get_speed_assignments_for_robot(data.robot_id)
        config_list = list(set([item['config_name'] for item in all_assignments]))
        composite_configs = "|".join(config_list)
    except Exception as e:
        logger.error(f"Error fetching assignments: {e}")
        composite_configs = data.config_name

    # 4. Wyślij do Kafki
    CONTROL_TOPIC = "speed_control"
    payload = {
        "robot_id": data.robot_id,
        "status": "ON",
        "config_names": composite_configs
    }
    
    try:
        await kafka._send_json(CONTROL_TOPIC, data.robot_id, payload, timestamp_ms=None)
    except Exception as e:
        logger.error(f"Kafka error: {e}")
        raise HTTPException(500, detail=f"Failed to send to Kafka: {str(e)}")
    
    return {
        "status": "enabled",
        "robot_id": data.robot_id,
        "config_name": data.config_name,
        "active_configs_sent": composite_configs
    }


# ==========================================
# OPCJA 1: PRECYZYJNE USUWANIE (To jest "Smart Delete")
# To jest to, czego używasz na co dzień w UI
# ==========================================
@router.delete("/speed", tags=["Control"], summary="Disable specific speed monitoring")
async def disable_speed_monitoring(
    data: SpeedRequest,
    kafka: KafkaService = Depends(get_kafka_service)
):
    # 1. Usuń tylko to jedno przypisanie
    database.remove_speed_assignment(data.robot_id, data.config_name)
    
    # 2. Sprawdź czy coś zostało i wyślij aktualizację (logika z poprzedniej odpowiedzi)
    remaining = database.get_speed_assignments_for_robot(data.robot_id)
    
    if not remaining:
        payload = {"status": "OFF"}
    else:
        # Sklejamy pozostałe configi, np. "speed2|speed3"
        configs = "|".join(set([r['config_name'] for r in remaining]))
        payload = {
            "robot_id": data.robot_id,
            "status": "ON",
            "config_names": configs
        }

    await kafka._send_json("speed_control", data.robot_id, payload, timestamp_ms=None)
    return {"status": "updated", "current_state": payload}


# ==========================================
# OPCJA 2: NUKLEARNY DELETE (Reset)
# Tego używasz w panelu admina lub w sytuacjach awaryjnych
# ==========================================
@router.delete("/speed/reset/{robot_id}", tags=["Control"], summary="HARD RESET: Disable ALL monitoring for robot")
async def reset_robot_monitoring(
    robot_id: str,
    kafka: KafkaService = Depends(get_kafka_service)
):
    """
    UWAGA: To usuwa robota ze WSZYSTKICH konfiguracji prędkości.
    Wysyła status OFF do Kafki i czyści bazę danych dla tego robota.
    """
    
    # 1. Wyczyść wszystko w bazie dla tego robota (będziesz musiał dodać taką funkcję w database.py)
    # np. DELETE FROM speed_assignments WHERE robot_id = ?
    try:
        # Zakładamy, że dodasz tę funkcję do database.py
        database.remove_all_speed_assignments_for_robot(robot_id)
    except Exception as e:
        logger.error(f"DB Error during reset: {e}")
        raise HTTPException(500, detail=str(e))

    # 2. Wyślij bezwarunkowe OFF do Kafki
    payload = {"status": "OFF"}
    
    # Nadpisujemy stan w ksqlDB - robot znika z tabeli aktywnych
    await kafka._send_json("speed_control", robot_id, payload, timestamp_ms=None)
    
    logger.warning(f"NUCLEAR RESET performed for robot: {robot_id}")
    return {"status": "reset_complete", "robot_id": robot_id}


@router.get("/speed", tags=["Control"], response_model=SpeedListResponse, summary="List active speed monitoring")
def list_speed_monitoring():
    try:
        assignments = database.get_all_speed_assignments()
        assignment_list = [
            SpeedAssignment(
                id=a['id'],
                robot_id=a['robot_id'],
                config_name=a['config_name'],
                status="ACTIVE"
            )
            for a in assignments
        ]
        return SpeedListResponse(
            total_count=len(assignment_list),
            assignments=assignment_list
        )
    except Exception as e:
        logger.error(f"Error fetching assignments: {e}")
        raise HTTPException(500, detail=f"Failed to fetch assignments: {str(e)}")


@router.get("/speed/{robot_id}", tags=["Control"], summary="Get speed monitoring for specific robot")
def get_speed_monitoring_for_robot(robot_id: str):
    try:
        assignments = database.get_speed_assignments_for_robot(robot_id)
        
        if not assignments:
            return {
                "robot_id": robot_id,
                "is_monitored": False,
                "config_name": None
            }
        
        return {
            "robot_id": robot_id,
            "is_monitored": True,
            # Zwracamy listę oddzieloną przecinkami lub pierwszy element
            "config_name": assignments[0]['config_name'], 
            "active_configs_count": len(assignments),
            "assignments": assignments
        }
    except Exception as e:
        logger.error(f"Error: {e}")
        raise HTTPException(500, detail=str(e))