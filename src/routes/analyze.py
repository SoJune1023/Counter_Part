import os
import sys

from fastapi import APIRouter, status, File, UploadFile, Form, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from typing import Annotated

from src.schemas import AnalyzeRequestSchema
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
async def post_analyze_get_tb_table(
    file: Annotated[UploadFile, File()],
    statement_id_col: Annotated[str, File()],
    account_id_col: Annotated[str, File()],
    debit_col: Annotated[str, File()],
    credit_col: Annotated[str, File()],
    summary_col: Annotated[str, File()]
):
    """Get data for main page"""

    data = AnalyzeRequestSchema(
        statement_id_col=statement_id_col,
        account_id_col=account_id_col,
        debit_col=debit_col,
        credit_col=credit_col,
        summary_col=summary_col
    )

    return await analyze_get_tb_table(file, data)

@route.post("/file/download", tags=['download'], status_code=status.HTTP_200_OK)
async def post_nalyze_get_tb_table_download(
    file: Annotated[UploadFile, File()],
    statement_id_col: Annotated[str, File()],
    account_id_col: Annotated[str, File()],
    debit_col: Annotated[str, File()],
    credit_col: Annotated[str, File()],
    summary_col: Annotated[str, File()]
):
    """Download simplification file."""

    data = AnalyzeRequestSchema(
        statement_id_col=statement_id_col,
        account_id_col=account_id_col,
        debit_col=debit_col,
        credit_col=credit_col,
        summary_col=summary_col
    )

    return await analyze_get_tb_table_download(file, data)

@route.post("/account_table", tags=['account_table'], status_code=status.HTTP_200_OK)
async def post_analyze_account_table(
    file: Annotated[UploadFile, File()],
    selected_account_id: Annotated[str, File()],
    classification_col: Annotated[str, File()],
    statement_id_col: Annotated[str, File()],
    account_id_col: Annotated[str, File()],
    debit_col: Annotated[str, File()],
    credit_col: Annotated[str, File()],
    summary_col: Annotated[str, File()]
):
    """Get excel file and return analyze result."""

    data = AnalyzeRequestSchema(
        selected_account_id=selected_account_id,
        classification_col=classification_col,
        statement_id_col=statement_id_col,
        account_id_col=account_id_col,
        debit_col=debit_col,
        credit_col=credit_col,
        summary_col=summary_col
    )

    return await analyze_account_table(file, data)

@route.post("/account_table/download", tags=['download'], status_code=status.HTTP_200_OK)
async def post_analyze_account_table_download(
    file: Annotated[UploadFile, File()],
    selected_account_id: Annotated[str, File()],
    classification_col: Annotated[str, File()],
    statement_id_col: Annotated[str, File()],
    account_id_col: Annotated[str, File()],
    debit_col: Annotated[str, File()],
    credit_col: Annotated[str, File()],
    summary_col: Annotated[str, File()]
):
    """Get excel file and return analyze file."""

    data = AnalyzeRequestSchema(
        selected_account_id=selected_account_id,
        classification_col=classification_col,
        statement_id_col=statement_id_col,
        account_id_col=account_id_col,
        debit_col=debit_col,
        credit_col=credit_col,
        summary_col=summary_col
    )

    return await analyze_account_table_download(file, data)