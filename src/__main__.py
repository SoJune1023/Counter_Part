import os
import sys
import webbrowser
import asyncio
import socket
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

async def run():
    # Set logging
    from src.config import init_logger
    logger = logging.getLogger(__name__)
    init_logger()

    for router in ALL_ROUTER:
        app.include_router(router)
    logger.info("Success to add router.")    

    host = "127.0.0.1"
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, 0))
    sock.setblocking(False)

    actual_port = sock.getsockname()[1]
    url = f"http://{host}:{actual_port}/analyze"

    config = uvicorn.Config(
        app,
        host=host,
        port=actual_port,
        fd=sock.fileno()
    )
    server = uvicorn.Server(config)

    loop = asyncio.get_event_loop()
    loop.call_later(0.5, lambda: webbrowser.open(url))

    try:
        await server.serve()
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        sock.close()

if __name__ == '__main__':
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass