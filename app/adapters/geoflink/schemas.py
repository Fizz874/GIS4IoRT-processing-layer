from pydantic import BaseModel, Field, model_validator
from typing import List, Literal, Union, Optional

class BaseJobConfig(BaseModel):
    name: str
    parallelism: int = Field(default=1, ge=1, description="Flink parallelism must be >= 1")    
    bootStrapServers: str = "broker:29092"

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
            "--cellLengthMeters", str(self.cellLengthMeters),
            "--uniformGridSize", str(self.uniformGridSize),
            "--gridMinX", str(self.gridMinX),
            "--gridMinY", str(self.gridMinY),
            "--gridMaxX", str(self.gridMaxX),
            "--gridMaxY", str(self.gridMaxY),
            "--inputTopicName", self.inputTopicName,
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
    robot_id: Optional[str] = None
    zone_id: Optional[str] = None
    config_name: str


    
# for SQL
class RobotEntry(BaseModel):
    id: str
    status: str

class ZoneEntry(BaseModel):
    id: str
    geo: str #WKB hex


# for API
class ZoneCreate(BaseModel):
    id: str
    geo: str

class RobotCreate(BaseModel):
    id: str
