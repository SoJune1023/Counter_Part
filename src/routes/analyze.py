import os
import sys

from fastapi import APIRouter, status, File, UploadFile, Form, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse

from src.handlers import (
    analyze_account_table, analyze_account_table_download,
    analyze_get_tb_table, analyze_get_tb_table_download
)

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
    return templates.TemplateResponse("index.html", {"request": request})

@route.post("/file", tags=['file'], status_code=status.HTTP_200_OK)
async def post_nalyze_get_tb_table(
    file: UploadFile = File(...)
):
    """Get data for main page"""
    return await analyze_get_tb_table(file)

@route.post("/file/download", tags=['download'], status_code=status.HTTP_200_OK)
async def post_nalyze_get_tb_table_download(
    file: UploadFile = File(...)
):
    """Download simplification file."""
    return await analyze_get_tb_table_download(file)

@route.post("/account_table", tags=['account_table'], status_code=status.HTTP_200_OK)
async def post_analyze_account_table(
    file: UploadFile = File(...),
    account_name: str = Form(...)
):
    """Get excel file and return analyze result."""
    return await analyze_account_table(file, account_name)

@route.post("/account_table/download", tags=['download'], status_code=status.HTTP_200_OK)
async def post_analyze_account_table_download(
    file: UploadFile = File(...),
    account_name: str = Form(...)
):
    """Get excel file and return analyze file."""
    return await analyze_account_table_download(file, account_name)