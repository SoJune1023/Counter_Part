import polars as pl
import io

from unittest.mock import patch

from src.services.analyze_account_table import analyze_account_table 

def test_analyze_account_table_success():
    mock_data = {
        "전표번호": ["2026-001", "2026-001", "2026-002"],
        "계정과목": ["보통예금", "소모품비", "임차료"],
        "차변": [0, 50000, 1000000],
        "대변": [50000, 0, 0],
        "적요": ["사무용품", "사무용품", "월세"]
    }
    mock_df = pl.DataFrame(mock_data)

    with patch("polars.read_excel") as mock_read:
        mock_read.return_value = mock_df
        
        fake_file = io.BytesIO(b"fake excel data")
        
        result = analyze_account_table(fake_file, "보통예금")

        assert result.height == 2
        assert "2026-001" in result["전표번호"].to_list()
        assert "소모품비" in result["계정과목"].to_list()
        assert "임차료" not in result["계정과목"].to_list()