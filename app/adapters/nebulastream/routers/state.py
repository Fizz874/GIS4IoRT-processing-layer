import httpx
import os
from fastapi import APIRouter, HTTPException
from app.config import settings

router = APIRouter(prefix="/state", tags=["NebulaStream State"])

@router.get("")
async def get_nes_state():
    coordinator_ip = settings.NES_COORDINATOR_IP
    coordinator_port = settings.NES_COORDINATOR_REST_PORT
    
    url = f"http://{coordinator_ip}:{coordinator_port}/v1/nes/connectivity/check"
    
    async with httpx.AsyncClient() as client:
        try:
            print(url)
            response = await client.get(url, timeout=5.0)
            if response.status_code == 200:
                data = response.json()
                if data.get("success") is True:
                    return {
                        "status": "NebulaStream OK",
                        "coordinator_response": data
                    }
            
            return {
                "status": "NebulaStream Down",
                "http_status_code": response.status_code
            }
            
        except httpx.RequestError as exc:
            raise HTTPException(
                status_code=503, 
                detail=f"Error connecting with NebulaStream: {exc}"
            )