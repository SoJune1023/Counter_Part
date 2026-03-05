import io
import asyncio
import logging
logger = logging.getLogger(__name__)

from fastapi import UploadFile
from fastapi.responses import StreamingResponse, Response

from src.services import AnalyzeAccountTable, AnalyzeGetExcel
from src.exceptions import InternalServerError, ClientError

async def analyze_page(file: UploadFile):
    try:
        loader = AnalyzeGetExcel()

        content = await file.read()

        loop = asyncio.get_running_loop()

        await file.seek(0)

        result = await loop.run_in_executor(
            None,
            loader.process,
            content
        )

        output = io.BytesIO()
        result.write_ipc(output)
        output.seek(0)
        
        return Response(
            content=output.getvalue(),
            media_type="application/octet-stream"
        )
    except (ClientError, InternalServerError):
        raise
    except Exception as e:
        logger.error("Unexpected exception detected while get excel data. Please contact me. email: ssojune@naver.com.", exc_info=True)
        raise InternalServerError(f"Unexpected exception on /analyze GET. Exc info: {str(e)}", 500) from e

async def account_table(file: UploadFile, account_name: str):
    try:
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
    except (ClientError, InternalServerError):
        raise
    except Exception as e:
        logger.error("Unexpected exception detected while analyze data. Please contact me. email: ssojune@naver.com.", exc_info=True)
        raise InternalServerError(f"Unexpected exception on /analyze/account_table POST. Exc info: {str(e)}", 500) from e