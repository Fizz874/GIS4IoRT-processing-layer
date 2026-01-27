import subprocess
import asyncio
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/collision", tags=["NebulaStream Collision"])

class CollisionRequest(BaseModel):
    Buffer: str
    QueryName: str

@router.post("/run")
async def run_sensor_query(request: CollisionRequest):
    command = [
        "java", "-jar", "/jars/nes_collision.jar",
        request.Buffer,
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
            detail="No jar /jars/nes_collision.jar"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Error: {str(e)}"
        )