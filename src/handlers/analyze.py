import io
import asyncio

from fastapi import UploadFile
from fastapi.responses import StreamingResponse

from src.services import analyze_account_table

async def account_table(file: UploadFile, account_name: str):
    # analyze
    loop = asyncio.get_event_loop()

    await file.seek(0)
    result = await loop.run_in_executor(
        None,
        analyze_account_table,
        file.file,
        account_name
    )

    # upload result file to memory
    output = io.BytesIO()
    result.write_excel(output)
    output.seek(0)

    # set file name
    filename = f"result_{file.filename}"

    # return result
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )