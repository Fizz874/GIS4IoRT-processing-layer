from pydantic import BaseModel, Field, model_validator, validator
from typing import List, Literal, Union, Optional
import re

class BaseJobConfig(BaseModel):
    name: str
    parallelism: int = Field(default=1, ge=1, description="Flink parallelism must be >= 1")    
    bootStrapServers: str = "broker:29092"
    localWebUi: bool = Field(default=False, description="Start local Web UI (for debug only)")

    def to_flink_args(self, control_topic: str, output_topic: str) -> List[str]:
        raise NotImplementedError()
    
    def get_entry_class(self) -> str:
        raise NotImplementedError("Each config type must define its own Entry Class!")

class GeofenceConfig(BaseJobConfig):
    type: Literal["GEOFENCE"]
    cellLengthMeters: float = Field(default=0, ge=0)
    uniformGridSize: int = Field(default=100, ge=0)
    gridMinX: float = 3.430
    gridMinY: float = 46.336
    gridMaxX: float = 3.436
    gridMaxY: float = 46.342
    inputTopicName: str = "multi_gps_fix"
    approximateQuery: bool = False
    omegaDuration: int = Field(default=1, ge=1, description="Window size and slide step in seconds")

    @model_validator(mode='after')
    def check_coordinates(self):
        if self.gridMinX >= self.gridMaxX:
            raise ValueError(f"gridMinX ({self.gridMinX}) must be smaller than gridMaxX ({self.gridMaxX})")
        if self.gridMinY >= self.gridMaxY:
            raise ValueError(f"gridMinY ({self.gridMinY}) must be smaller than gridMaxY ({self.gridMaxY})")
        return self


    def to_flink_args(self, control_topic: str, output_topic: str) -> list[str]:
        return [
            "--parallelism", str(self.parallelism),
            "--bootStrapServers", self.bootStrapServers,
            "--localWebUi", str(self.localWebUi).lower(),

            "--cellLengthMeters", str(self.cellLengthMeters),
            "--uniformGridSize", str(self.uniformGridSize),
            "--gridMinX", str(self.gridMinX),
            "--gridMinY", str(self.gridMinY),
            "--gridMaxX", str(self.gridMaxX),
            "--gridMaxY", str(self.gridMaxY),
            "--inputTopicName", self.inputTopicName,
            "--approximateQuery", str(self.approximateQuery).lower(), 
            "--omegaDuration", str(self.omegaDuration),
            "--outputTopicName", output_topic,
            "--controlTopicName", control_topic
        ]

    def get_entry_class(self) -> str:
        return "GIS4IoRT.jobs.ParcelControlStreamingJob"

# Example of a theoretical different query - just for reference

# class CollisionConfig(BaseJobConfig):
#     type: Literal["COLLISION"]
#     warning_distance_meters: float = 2.0  # Inne parametry niż w Geofence!
    
#     def to_flink_args_list(self, control_topic: str) -> List[str]:
#         return [
#             "--algorithm", "COLLISION",
#             "--parallelism", str(self.parallelism),
#             "--threshold", str(self.warning_distance_meters),
#             "--controlTopicName", control_topic
#         ]

JobConfigUnion = Union[GeofenceConfig] #, CollisionConfig


class GeofenceRequest(BaseModel):
    robot_id: str = Field(..., min_length=1, description="ID robota z bazy danych")
    zone_id: str = Field(..., min_length=1, description="ID strefy z bazy danych")
    config_name: str = Field(..., min_length=1)


    
# for SQL
class RobotEntry(BaseModel):
    id: str
    status: str

class ZoneEntry(BaseModel):
    id: str
    geo: str #WKB hex


# for API
class ZoneCreate(BaseModel):
    id: str = Field(
        ..., 
        min_length=1, 
        strip_whitespace=True,
        description="Unique zone ID"
    )
    
    geo: str = Field(
        ..., 
        min_length=10,
        description="Geometry in WKB HEX String Format"
    )

    @validator('geo')
    def validate_wkb_hex(cls, v):
        if not re.fullmatch(r'^[0-9A-Fa-f]+$', v):
            raise ValueError('Geometry has to be a correct HEX String')
        
        if len(v) % 2 != 0:
            raise ValueError('Wrong length of the WKB HEX String (has to be even)')
            
        return v.upper()

class RobotCreate(BaseModel):
    id: str = Field(
        ..., 
        min_length=1, 
        max_length=100,           
        strip_whitespace=True,
        description="Unique robot ID"
    )