from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.adapters.geoflink import database
import logging
from contextlib import asynccontextmanager, AsyncExitStack

#uvicorn app.main:app --reload

# Import different technology adapters
from app.adapters.geoflink.routers import router as geoflink_router
from app.adapters.geoflink.lifecycle import geoflink_lifespan


logger = logging.getLogger("uvicorn.info")

@asynccontextmanager
async def lifespan(app: FastAPI):


    async with AsyncExitStack() as stack:
       
        await stack.enter_async_context(geoflink_lifespan(app))
        
        yield
   

app = FastAPI(
    title="GIS4IoRT API",
    lifespan=lifespan
)

app.include_router(geoflink_router, prefix="/geoflink")

@app.get("/", tags=["Health"])
def root():
    return {
        "system": "GIS4IoRT",
        "status": "RUNNING",
        "docs": "/docs"
    }
