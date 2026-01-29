from fastapi import APIRouter, HTTPException, Depends
from app.adapters.ksqldb import database
from app.adapters.ksqldb.schemas import RobotCreate
from app.adapters.ksqldb.kafka_service import KafkaService, get_kafka_service
router = APIRouter()


@router.post("/robots", tags=["Management"])
async def add_robot(
    robot: RobotCreate,
    kafka: KafkaService = Depends(get_kafka_service)
):
    database.upsert_robot(database.RobotEntry(robot.id, "REGISTERED"))
    
    await kafka.register_robot(robot.id)
    
    return {"status": "registered", "id": robot.id}

@router.get("/robots", tags=["Management"])
def list_robots():
    return database.get_all_robots()

@router.delete("/robots/{robot_id}", tags=["Management"])
async def delete_robot(
    robot_id: str,
    kafka: KafkaService = Depends(get_kafka_service)
):
    database.delete_robot(robot_id)
    
    await kafka.deregister_robot(robot_id)
    
    return {"status": "deleted"}