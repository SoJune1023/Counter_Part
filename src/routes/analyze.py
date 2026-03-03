from fastapi import APIRouter, status, File, UploadFile, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from src.handlers import account_table

templates = Jinja2Templates(directory="src/templates")

route = APIRouter(
    prefix="/analyze",
    tags=['analyze']
)

@route.get("/", response_class=HTMLResponse)
async def get_analyze_page(request: Request):
    """Serve main page"""
    return templates.TemplateResponse("index.html", {"request": request})

@route.post("/account_table", tags=['account_table'], status_code=status.HTTP_200_OK)
async def post_account_table(
    file: UploadFile = File(...),
    account_name: str = Form(...)
):
    """Get excel file and return analyze result."""
    return await account_table(file, account_name)