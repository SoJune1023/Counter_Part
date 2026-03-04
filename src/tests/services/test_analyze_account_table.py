import polars as pl
import io
import pytest

from unittest.mock import patch
from polars.exceptions import ShapeError

from src.services.analyze_account_table import AnalyzeAccountTable 
from src.exceptions import ClientError

@pytest.fixture
def analyze_account_table():
    return AnalyzeAccountTable()

def test_analyze_account_table_success(analyze_account_table):
    mock_data = {
        "전표번호": ["2026-001", "2026-001", "2026-002"],
        "계정과목": ["소모품비", "보통예금", "보통예금"],
        "차변": [50000, 0, 70000],
        "대변": [0, 50000, 0],
        "적요": ["사무용품", "이체", "입금"]
    }
    mock_df = pl.DataFrame(mock_data)

    with patch("polars.read_excel") as mock_read:
        mock_read.return_value = mock_df
        
        fake_file = io.BytesIO(b"fake excel data")
        
        result = analyze_account_table.process(fake_file, "보통예금")

    expected_result = pl.DataFrame([
        {
            "전표번호": "2026-001",
            "분류": None,
            "적요": "사무용품",
            "소모품비(차변)": 50000,
            "보통예금(차변)": 0,
            "보통예금(대변)": 0
        },
        {
            "전표번호": "2026-001",
            "분류": None,
            "적요": "이체",
            "소모품비(차변)": 0,
            "보통예금(차변)": 0,
            "보통예금(대변)": 50000
        },
        {
            "전표번호": "2026-002",
            "분류": None,
            "적요": "입금",
            "소모품비(차변)": 0,
            "보통예금(차변)": 70000,
            "보통예금(대변)": 0
        }
    ]).select([
        "전표번호", "분류", "적요", "소모품비(차변)", "보통예금(차변)", "보통예금(대변)"
    ]).sort(["전표번호", "적요"])


    assert result.select(expected_result.columns).equals(expected_result)

def test__read_excel_success(analyze_account_table):
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
        
        result = analyze_account_table._read_excel(fake_file, statement_id_col="전표번호")

        assert result.collect().equals(expected_result.collect())

def test__read_excel_failed_shape_error(analyze_account_table):
    with patch("polars.read_excel") as mock_read:
        mock_read.side_effect = ShapeError("<shape_error_info>")
        
        fake_file = io.BytesIO(b"fake excel data")
        
        with pytest.raises(ClientError, match="Could not create dataform."):
            analyze_account_table._read_excel(fake_file, statement_id_col="전표번호")

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

def test__collect_success(analyze_account_table):
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

    raw_data = (
        mock_lf.join(statement_ids, on="전표번호", how="semi")
        .select([
            "전표번호",
            "계정과목",
            "차변",
            "대변",
            "적요"
        ])
        .sort("전표번호")
    )

    result = analyze_account_table._collect(raw_data)

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

def test__pivot_success(analyze_account_table):
    mock_data = {
        "전표번호": ["2026-001", "2026-001", "2026-002"],
        "계정과목": ["소모품비", "보통예금", "보통예금"],
        "차변": [50000, 0, 70000],
        "대변": [0, 50000, 0],
        "적요": ["사무용품", "이체", "입금"]
    }
    mock_lf = pl.DataFrame(mock_data).lazy()

    statement_ids = (
        mock_lf.filter(pl.col("계정과목") == "보통예금")
        .select("전표번호")
        .unique()
    )

    raw_data = (
        mock_lf.join(statement_ids, on="전표번호", how="semi")
        .select([
            "전표번호",
            "계정과목",
            "차변",
            "대변",
            "적요"
        ])
        .sort("전표번호")
    )

    collected_data = raw_data.collect()

    result = analyze_account_table._pivot(collected_data)

    expected_result = pl.DataFrame([
        {
            "전표번호": "2026-001",
            "분류": None,
            "적요": "사무용품",
            "소모품비(차변)": 50000,
            "보통예금(차변)": 0,
            "보통예금(대변)": 0
        },
        {
            "전표번호": "2026-001",
            "분류": None,
            "적요": "이체",
            "소모품비(차변)": 0,
            "보통예금(차변)": 0,
            "보통예금(대변)": 50000
        },
        {
            "전표번호": "2026-002",
            "분류": None,
            "적요": "입금",
            "소모품비(차변)": 0,
            "보통예금(차변)": 70000,
            "보통예금(대변)": 0
        }
    ]).select([
        "전표번호", "분류", "적요", "소모품비(차변)", "보통예금(차변)", "보통예금(대변)"
    ]).sort(["전표번호", "적요"])


    assert result.select(expected_result.columns).equals(expected_result)