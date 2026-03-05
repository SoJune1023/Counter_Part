import os
import sys

from fastapi import APIRouter, status, File, UploadFile, Form, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse

from src.handlers import account_table, analyze_page

def get_resource_path(relative_path):
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, relative_path)
    return os.path.join(os.path.abspath("."), relative_path)

template_path = get_resource_path("static/templates") 
templates = Jinja2Templates(directory=template_path)

route = APIRouter(
    prefix="/analyze",
    tags=['analyze']
)

@route.get("/", response_class=HTMLResponse)
async def get_main_page(request: Request):
    """Serve main page"""
    return templates.TemplateResponse("analyze.html", {"request": request})


@route.post("/", status_code=status.HTTP_200_OK)
async def post_analyze_page(
    file: UploadFile = File(...)
):
    """Get data for main page"""
    return await analyze_page(file)

@route.get("/account_table", tags=['account_table'], status_code=status.HTTP_200_OK)
async def get_account_table(request: Request):
    """Get excel file and return analyze result."""
    return templates.TemplateResponse("analyze_table.html", {"request": request})

@route.post("/account_table", tags=['account_table'], status_code=status.HTTP_200_OK)
async def post_account_table(
    file: UploadFile = File(...),
    account_name: str = Form(...)
):
    """Get excel file and return analyze result."""
    return await account_table(file, account_name)