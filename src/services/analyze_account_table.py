import polars as pl

from typing import IO

class AnalyzeAccountTable:
    def _get_statement_ids(
        self,
        lf: pl.LazyFrame,
        *,
        account_name: str,
        statement_id_col: str = "전표번호",
        account_col: str = "계정과목"
    ) -> pl.LazyFrame:
        """Get statement numbers WHERE account_col is account_name."""
        return (
            lf.filter(pl.col(account_col) == account_name)
            .select(statement_id_col)
            .unique()
        )
    
    def _get_raw_data(
        self,
        statement_ids: pl.LazyFrame,
        lf: pl.LazyFrame,
        *,
        statement_id_col: str = "전표번호",
        account_col: str = "계정과목",
        chabun_col: str = "차변",
        daebun_col: str = "대변",
        summary_col: str = "적요"
    ) -> pl.LazyFrame:
        """Get data that WHERE statement_id is in statement_ids."""
        return (
            lf.join(statement_ids, on=statement_id_col, how="semi")
            .select([
                statement_id_col,
                account_col,
                chabun_col,
                daebun_col,
                summary_col
            ])
            .sort(statement_id_col)
        )

    def process(
        self,
        file_ptr: IO[bytes],
        account_name: str,
        *,
        statement_id_col: str = "전표번호",
        account_col: str = "계정과목",
        chabun_col: str = "차변",
        daebun_col: str = "대변",
        summary_col: str = "적요"
    ):
        df = pl.read_excel(file_ptr, engine="calamine")
        lf = df.lazy().with_columns(
            pl.col(statement_id_col).cast(pl.Utf8)
        )

        statement_ids = self._get_statement_ids(
            lf,
            account_name=account_name,
            statement_id_col=statement_id_col,
            account_col=account_col
        )

        raw_data = self._get_raw_data(
            statement_ids,
            lf,
            statement_id_col=statement_id_col,
            account_col=account_col,
            chabun_col=chabun_col,
            daebun_col=daebun_col,
            summary_col=summary_col
        )

        """Expected raw_data:
        | 전표번호   | 계정과목 | 차변    | 대변    | 적요
        | 2026-001 | 소모품비 | 50,000 | 0      | ...
        | 2026-001 | 보통예금 | 0      | 50,000 | ...
        """

        return raw_data