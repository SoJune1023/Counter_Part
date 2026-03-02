from fastapi import APIRouter, status, File, UploadFile

from src.handlers import account_table

route = APIRouter(
    prefix="/analyze",
    tags=['analyze']
)

@route.post("/account_table", tags=['account_table'], status_code=status.HTTP_200_OK)
async def post_account_table(
    file: UploadFile = File(...)
):
    """Get excel file and return analyze result."""
    return await account_table(file)