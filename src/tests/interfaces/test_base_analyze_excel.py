import io
import pytest
import polars as pl

from unittest.mock import patch
from polars.exceptions import ShapeError

from src.exceptions import ClientError
from src.schemas import AnalyzeRequestSchema

from src.interfaces.base_analyze_excel import BaseAnalyzeExcel

class TestAnalyzeExcel(BaseAnalyzeExcel):
    def _get_data(
        self,
        lf: pl.LazyFrame,
        *args,
        data
    ) -> pl.LazyFrame:
        pass

    def _pivot(
        self,
        df: pl.DataFrame,
        *args,
        data
    ) -> pl.DataFrame:
        pass

@pytest.fixture
def test_analyze_excel() -> TestAnalyzeExcel:
    return TestAnalyzeExcel()

kwargs = {
    "statement_id_col": "전표번호",
    "account_id_col": "계정과목",
    "debit_col": "차변",
    "credit_col": "대변",
    "summary_col": "적요"
}

data = AnalyzeRequestSchema(**kwargs)

def test_read_excel_success(test_analyze_excel):
    mock_data = {
        "전표번호": ["2026-001", "2026-001", "2026-002"],
        "계정과목": ["소모품비", "보통예금", "보통예금"],
        "차변": [50000, 0, 70000],
        "대변": [0, 50000, 0],
        "적요": ["사무용품", "이체", "입금"]
    }
    mock_df = pl.DataFrame(mock_data)

    expected_result = mock_df.lazy().with_columns(
        pl.col("전표번호").cast(pl.Utf8)
    )

    with patch("polars.read_excel") as mock_read:
        mock_read.return_value = mock_df
        
        fake_file = io.BytesIO(b"fake excel data")
        
        result = test_analyze_excel._read_excel(fake_file, **kwargs)

        assert result.collect().equals(expected_result.collect())

def test__read_excel_failed_shape_error(test_analyze_excel):
    with patch("polars.read_excel") as mock_read:
        mock_read.side_effect = ShapeError("<shape_error_info>")
        
        fake_file = io.BytesIO(b"fake excel data")
        
        with pytest.raises(ClientError, match="Could not create dataform."):
            test_analyze_excel._read_excel(fake_file,  **kwargs)

def test__read_excel_failed_no_data(test_analyze_excel):
    mock_data = {
        "전표번호": [],
        "계정과목": [],
        "차변": [],
        "대변": [],
        "적요": []
    }
    mock_df = pl.DataFrame(mock_data)

    with patch("polars.read_excel") as mock_read:
        mock_read.return_value = mock_df
        
        fake_file = io.BytesIO(b"fake excel data")
        
        with pytest.raises(ClientError, match="There is no data to get"):
            test_analyze_excel._read_excel(fake_file,  **kwargs)

def test__collect_success(test_analyze_excel):
    mock_data = {
        "전표번호": ["2026-001", "2026-001", "2026-002"],
        "계정과목": ["소모품비", "보통예금", "보통예금"],
        "차변": [50000, 0, 70000],
        "대변": [0, 50000, 0],
        "적요": ["사무용품", "이체", "입금"]
    }

    mock_df = pl.DataFrame(mock_data)
    mock_lf = mock_df.lazy().with_columns(
        pl.col("전표번호").cast(pl.Utf8)
    )

    expected_result = mock_lf.collect()

    result = test_analyze_excel._collect(mock_lf)

    assert result.equals(expected_result)