from fastapi import APIRouter, status, File, UploadFile, Form

from src.handlers import account_table, analyze_page

route = APIRouter(
    prefix="/analyze",
    tags=['analyze']
)

@route.post("/", status_code=status.HTTP_200_OK)
async def post_analyze_page(
    file: UploadFile = File(...)
):
    """Get data for main page"""
    return await analyze_page(file)

@route.post("/account_table", tags=['account_table'], status_code=status.HTTP_200_OK)
async def post_account_table(
    file: UploadFile = File(...),
    account_name: str = Form(...)
):
    """Get excel file and return analyze result."""
    return await account_table(file, account_name)