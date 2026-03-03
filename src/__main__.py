import uvicorn

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

app = FastAPI()

app.mount("/static", StaticFiles(directory="src/static"), name="static")

from src.routes import ALL_ROUTER

def run():
    for router in ALL_ROUTER:
        app.include_router(router)
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000
    )

if __name__ == '__main__':
    run()