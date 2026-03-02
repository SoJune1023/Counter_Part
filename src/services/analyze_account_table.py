import polars as pl

from typing import IO

def analyze_account_table(
    file_ptr: IO[bytes],
    account_name: str,
    *,
    statement_num_col: str = "전표번호",
    account_col: str = "계정과목",
    chabun_col: str = "차변",
    daebun_col: str = "대변",
    summary_col: str = "적요"
) -> pl.DataFrame:
    df = pl.read_excel(file_ptr, engine="calamine")
    lf = df.lazy().with_columns(
        pl.col(statement_num_col).cast(pl.Utf8)
    )

    statement_numbers = ( # Get statement numbers as series WHERE account_col is account_name.
        lf.filter(pl.col(account_col) == account_name)
        .select(statement_num_col)
        .unique()
        .collect()
        .to_series()
        .to_list()
    )

    result = ( # Get data that we need.
        lf.filter(pl.col(statement_num_col).is_in(statement_numbers))
        .select([
            statement_num_col,
            account_col,
            chabun_col,
            daebun_col,
            summary_col
        ])
        .sort(statement_num_col)
        .collect()
    )
    """Expected result:
    | 전표번호   | 계정과목 | 차변    | 대변    | 적요
    | 2026-001 | 소모품비 | 50,000 | 0      | ...
    | 2026-001 | 보통예금 | 0      | 50,000 | ...
    """

    return result