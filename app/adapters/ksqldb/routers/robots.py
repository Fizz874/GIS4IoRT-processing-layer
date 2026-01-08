from fastapi import APIRouter, HTTPException
from app.adapters.ksqldb import database
from app.adapters.ksqldb.schemas import RobotCreate

router = APIRouter()

@router.post("/robots", tags=["Management"])
def add_robot(robot: RobotCreate):
    database.upsert_robot(database.RobotEntry(robot.id, "REGISTERED"))
    return {"status": "registered", "id": robot.id}

# Returns database status
@router.get("/robots", tags=["Management"])
def list_robots():
    return database.get_all_robots()

@router.delete("/robots/{robot_id}", tags=["Management"])
def delete_robot(robot_id: str):
    database.delete_robot(robot_id)
    return {"status": "deleted"}