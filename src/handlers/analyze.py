import io
import asyncio

from fastapi import UploadFile
from fastapi.responses import StreamingResponse

from src.services import AnalyzeAccountTable

async def account_table(file: UploadFile, account_name: str):
    analyzer = AnalyzeAccountTable()

    content = await file.read()

    loop = asyncio.get_running_loop()

    await file.seek(0)

    result = await loop.run_in_executor(
        None,
        analyzer.process,
        content,
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