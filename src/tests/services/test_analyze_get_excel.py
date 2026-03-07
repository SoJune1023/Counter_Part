import io
import polars as pl
import pytest

from unittest.mock import patch
from polars.exceptions import ShapeError, ColumnNotFoundError

from src.services.analyze_get_excel import AnalyzeGetExcel
from src.exceptions import ClientError, InternalServerError

@pytest.fixture
def analyze_get_excel() -> AnalyzeGetExcel:
    return AnalyzeGetExcel()

def test_process_success(analyze_get_excel):
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

        result = analyze_get_excel.process(fake_file)

    expected_result_data = mock_data = {
        "전표번호": ["2026-001", "2026-002"],
        "차변": [50000, 70000],
        "대변": [50000, 0],
        "적요": ["사무용품", "입금"]
    }
    expected_result = pl.DataFrame(expected_result_data)

    assert result.select(expected_result.columns).equals(expected_result)

def test_process_failed_wrong_column_name(analyze_get_excel):
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
            analyze_get_excel.process(fake_file)

# 나머지 테스트는 /src/tests/serivces/test_analyze_account_table에서 진행하였기에 패스합니다. (동일 로직)