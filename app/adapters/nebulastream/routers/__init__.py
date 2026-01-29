from fastapi import APIRouter

from .geofence import router as geofence_router
from .sensor import router as sensor_router
from .collision import router as collision_router
from .state import router as state_router

router = APIRouter()

router.include_router(geofence_router)
router.include_router(collision_router)
router.include_router(sensor_router)
router.include_router(state_router)