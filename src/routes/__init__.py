import os
import sys

from fastapi.responses import HTMLResponse
from fastapi import Request, APIRouter
from fastapi.templating import Jinja2Templates

route = APIRouter()

def get_resource_path(relative_path):
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, relative_path)
    return os.path.join(os.path.abspath("."), relative_path)

template_path = get_resource_path("static/templates") 
templates = Jinja2Templates(directory=template_path)

from src.routes.analyze import route as analyze_route

@route.get("/", response_class=HTMLResponse)
async def get_main_page(request: Request):
    """Serve main page"""
    return templates.TemplateResponse("analyze.html", {"request": request})

ALL_ROUTER = [route, analyze_route]