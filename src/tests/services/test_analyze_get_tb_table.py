import io
import polars as pl
import pytest

from unittest.mock import patch
from polars.exceptions import ShapeError, ColumnNotFoundError

from src.services.analyze_get_tb_table import AnalyzeGetTbTable
from src.exceptions import ClientError, InternalServerError

@pytest.fixture
def analyze_get_tb_table() -> AnalyzeGetTbTable:
    return AnalyzeGetTbTable()

kwargs = {
    "statement_id_col": "전표번호",
    "account_id_col": "계정과목",
    "debit_col": "차변",
    "credit_col": "대변",
    "summary_col": "적요"
}

def test_process_success(analyze_get_tb_table):
    mock_data = {
        "전표번호": ["2026-001", "2026-001", "2026-002"],
        "계정과목": ["소모품비", "보통예금", "보통예금"],
        "차변": [50000, 0, 70000],
        "대변": [0, 50000, 0],
        "적요": ["사무용품", "이체", "입금"],
        "추가 데이터": ["data_1", "data_2", "data_3"]
    }
    mock_df = pl.DataFrame(mock_data)

    with patch("polars.read_excel") as mock_read:
        mock_read.return_value = mock_df
    
        fake_file = io.BytesIO(b"fake excel data")

        result = analyze_get_tb_table.process(fake_file, **kwargs)

    expected_result_data = mock_data = {
        "계정과목": ["보통예금", "소모품비"],
        "차변": [70000, 50000],
        "대변": [50000, 0]
    }
    expected_result = pl.DataFrame(expected_result_data)

    assert result.select(expected_result.columns).equals(expected_result)

def test_process_failed_wrong_column_name(analyze_get_tb_table):
    mock_data = {
        "전표번호": ["2026-001", "2026-001", "2026-002"],
        "계정-과목": ["소모품비", "보통예금", "보통예금"],
        "차변": [50000, 0, 70000],
        "대변": [0, 50000, 0],
        "적요": ["사무용품", "이체", "입금"]
    }
    mock_df = pl.DataFrame(mock_data)

    with patch("polars.read_excel") as mock_read:
        mock_read.return_value = mock_df
        
        fake_file = io.BytesIO(b"fake excel data")
        
        with pytest.raises(ClientError, match="Could not find column in file. Please check column name."):
            analyze_get_tb_table.process(fake_file, **kwargs)

# 나머지 테스트는 /src/tests/serivces/test_analyze_account_table에서 진행하였기에 패스합니다. (동일 로직)