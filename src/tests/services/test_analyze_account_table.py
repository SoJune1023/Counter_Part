import polars as pl
import io
import pytest

from unittest.mock import patch

from src.services.analyze_account_table import AnalyzeAccountTable 

@pytest.fixture
def analyze_account_table():
    return AnalyzeAccountTable()

def test_analyze_account_table_success(analyze_account_table):
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
        
        result = analyze_account_table.process(fake_file, "보통예금").collect()

        assert result.height == 2
        assert "2026-001" in result["전표번호"].to_list()
        assert "소모품비" in result["계정과목"].to_list()
        assert "임차료" not in result["계정과목"].to_list()

def test__get_statement_ids_success(analyze_account_table):
    mock_data = {
        "전표번호": ["2026-001", "2026-001", "2026-002", "2026-002", "2026-003"],
        "계정과목": ["보통예금", "보통예금", "임차료", "임차료" , "보통예금"],
        "차변": [0, 0, 2000000, 2000000, 0],
        "대변": [50000, 50000, 0, 0, 100000],
        "적요": ["사무용품", "사무용품", "월세", "월세", "기타용품"]
    }
    mock_lf = pl.DataFrame(mock_data).lazy()

    result = analyze_account_table._get_statement_ids(
        mock_lf,
        account_name="보통예금", 
        statement_id_col="전표번호", 
        account_col="계정과목"
    ).collect()
    # result = shape: (2,)\nSeries: '전표번호' [str]\n[\n      "2026-001"\n    "2026-003"\n]
    # 추후 to_series(), to_list() 해줘야 ["2026-001", "2026-003"] 로 반환

    expected_result = ["2026-001", "2026-003"]

    assert sorted(result["전표번호"]) == sorted(expected_result)

def test__get_raw_data_success(analyze_account_table):
    mock_data = {
        "전표번호": ["2026-001", "2026-001", "2026-002", "2026-002", "2026-003"],
        "계정과목": ["보통예금", "투자수익", "임차료", "임차료" , "보통예금"],
        "차변": [0, 50000, 2000000, 2000000, 0],
        "대변": [50000, 0, 0, 0, 100000],
        "적요": ["사무용품", "투자", "월세", "월세", "기타용품"]
    }
    mock_lf = pl.DataFrame(mock_data).lazy()

    statement_ids = (
        mock_lf.filter(pl.col("계정과목") == "보통예금")
        .select("전표번호")
        .unique()
    )
    # statement_ids = [shape: (2,)\nSeries: '전표번호' [str]\n[\n      "2026-001"\n    "2026-003"\n]] but in lazy state

    result = analyze_account_table._get_raw_data(
        statement_ids,
        mock_lf
    ).collect()

    expected_result = {
        "전표번호": ["2026-001", "2026-001", "2026-003"],
        "계정과목": ["보통예금", "투자수익", "보통예금"],
        "차변": [0, 50000, 0],
        "대변": [50000, 0, 100000],
        "적요": ["사무용품", "투자", "기타용품"]
    }

    assert sorted(result["전표번호"]) == sorted(expected_result["전표번호"])
    assert sorted(result["계정과목"]) == sorted(expected_result["계정과목"])
    assert sorted(result["차변"]) == sorted(expected_result["차변"])
    assert sorted(result["대변"]) == sorted(expected_result["대변"])
    assert sorted(result["적요"]) == sorted(expected_result["적요"])