import os
import sys
import webbrowser
import logging
import uvicorn

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

app = FastAPI()

def get_resource_path(relative_path):
    if hasattr(sys, '_MEIPASS'):
        # After build
        return os.path.join(sys._MEIPASS, relative_path)
    # Before build
    return os.path.join(os.path.abspath("."), relative_path)

static_path = get_resource_path("static")
app.mount("/static", StaticFiles(directory=static_path), name="static")

from src.routes import ALL_ROUTER

def run():
    # Set logging
    from src.config import init_logger
    logger = logging.getLogger(__name__)
    init_logger()

    for router in ALL_ROUTER:
        app.include_router(router)
    logger.info("Success to add router.")    

    url = "http://127.0.0.1:8000/analyze"
    webbrowser.open(url)
    
    uvicorn.run(
        app,
        host="127.0.0.1",
        port=8000
    )

if __name__ == '__main__':
    run()