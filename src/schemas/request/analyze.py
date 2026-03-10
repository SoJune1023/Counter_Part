from pydantic import BaseModel
from typing import Optional

class AnalyzeRequestSchema(BaseModel):
    selected_account_id: str = 'null'
    classification_col: str = 'null'
    statement_id_col: str
    account_id_col: str
    debit_col: str
    credit_col: str
    summary_col: str