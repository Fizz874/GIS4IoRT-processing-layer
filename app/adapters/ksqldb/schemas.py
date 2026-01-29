from pydantic import BaseModel
from typing import Optional

class GeofenceRequest(BaseModel):
    robot_id: Optional[str] = None
    zone_id: Optional[str] = None
    config_name: str

class ZoneCreate(BaseModel):
    id: str
    geo: str

class RobotCreate(BaseModel):
    id: str