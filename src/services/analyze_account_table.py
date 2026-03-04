import polars as pl

from typing import IO
from polars.exceptions import ShapeError

from src.exceptions import ClientError

class AnalyzeAccountTable:
    def _read_excel(
        self,
        file_ptr: IO[bytes],
        *,
        statement_id_col: str = "전표번호"
    ) -> pl.LazyFrame:
        try:
            df = pl.read_excel(file_ptr, engine="calamine")
            return df.lazy().with_columns(
                pl.col(statement_id_col).cast(pl.Utf8)
            )
        except ShapeError as e:
            raise ClientError(f"Could not create dataform.", 400) from e

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
            .unique() # delete overlap ids
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
    
    def _collect(
        self,
        raw_data: pl.LazyFrame
    ):
        """Return collect result."""
        return raw_data.collect()

    def _pivot(
        self,
        data: pl.DataFrame,
        *,
        statement_id_col: str = "전표번호",
        account_col: str = "계정과목",
        chabun_col: str = "차변",
        daebun_col: str = "대변",
        summary_col: str = "적요"
    ):
        """Pivot data to object schema and sort it. Then return."""
        data_with_idx = data.with_row_index("_row_id") # Prevent duplicate summation on Pivot

        chabun_data = data_with_idx.select([
            "_row_id",
            statement_id_col,
            summary_col,
            (pl.col(account_col) + pl.lit("(차변)")).alias("_account_type"), # Create new column name. example: 일반예금 -> 일반예금(차변)
            pl.col(chabun_col).alias("_amount") # Unify numbers in 'chabun_col' column to common column name '_amount'
        ]).filter(pl.col("_amount") > 0) # Extract only if amount > 0 (except for rows without 'chabun_col')

        daebun_data = data_with_idx.select([
            "_row_id",
            statement_id_col,
            summary_col,
            (pl.col(account_col) + pl.lit("(대변)")).alias("_account_type"), # Create new column name. example: 일반예금 -> 일반예금(대변)
            pl.col(daebun_col).alias("_amount") # Unify numbers in 'daebun_col' column to common column name '_amount'
        ]).filter(pl.col("_amount") > 0) # Extract only if amount > 0 (except for rows without 'daebun_col)

        combined = pl.concat([chabun_data, daebun_data])

        return (
            combined.with_columns(
                pl.lit(None).alias("분류") # Create new column '분류'
            )
            .pivot(
                index=["_row_id", statement_id_col, "분류", summary_col],
                on="_account_type",
                values="_amount",
                aggregate_function="first"
            )
            .fill_null(0)
            .drop("_row_id") # Remove useless value
            .sort([statement_id_col, summary_col])
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
        lf = self._read_excel(
            file_ptr,
            statement_id_col=statement_id_col
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

        collected_data = self._collect(raw_data)

        """Expected collected_data:
        | 전표번호   | 계정과목 | 차변    | 대변    | 적요
        | 2026-001 | 소모품비 | 50,000 | 0      | ...
        | 2026-001 | 보통예금 | 0      | 50,000 | ...
        | 2026-002 | 보통예금 | 70,000 | 0      | ...
        """

        pivot_data = self._pivot(
            collected_data,
            statement_id_col=statement_id_col,
            account_col=account_col,
            chabun_col=chabun_col,
            daebun_col=daebun_col,
            summary_col=summary_col
        )

        """Goal result:
        | 전표 번호  | 분류 | 소모품비(차변)  | 보통예금(차변) | 보통예금(대변) . . . etc
        | 2026-001 | n   | 50,000      | 70,000      | 0         
        | 2026-001 | n   | 0           | 0           | 50,000 
        | 2026-002 | n   | 0           | 0           | 0
        """

        return pivot_data