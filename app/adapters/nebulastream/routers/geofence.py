import subprocess
import asyncio
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/geofence", tags=["NebulaStream Geofence"])

class GeofenceRequest(BaseModel):
    WKBstring: str
    QueryName: str

@router.post("/run")
async def run_geofence_query(request: GeofenceRequest):
    command = [
        "java", "-jar", "/jars/nes_geofence.jar",
        request.WKBstring,
        request.QueryName
    ]
    
    try:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await process.communicate()
        
        result_output = stdout.decode().strip()
        error_output = stderr.decode().strip()
        
        if process.returncode != 0:
            return {
                "status": "error",
                "exit_code": process.returncode,
                "message": "Error",
                "details": error_output
            }
            
        return {
            "status": "success",
            "output": result_output
        }

    except FileNotFoundError:
        raise HTTPException(
            status_code=500, 
            detail="No jar /jars/nes_geofence.jar"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Error: {str(e)}"
        )