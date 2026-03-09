import polars as pl
import logging
logger = logging.getLogger(__name__)

from typing import IO
from polars.exceptions import ShapeError, ColumnNotFoundError

from src.exceptions import ClientError, InternalServerError

class AnalyzeAccountTable:
    def _read_excel(
        self,
        file_ptr: IO[bytes],
        *,
        statement_id_col: str = "전표번호"
    ) -> pl.LazyFrame:
        try:
            df = pl.read_excel(file_ptr, engine="calamine")

            if df.is_empty():
                logger.warning("There is nothing to get in file.")
                raise ClientError(f"There is no data to get.", 400)
            
            if statement_id_col not in df.columns:
                logger.warning(f"Failed to get column name '{statement_id_col}' in file.")
                raise ClientError(f"Failed to get column name '{statement_id_col}' in file.", 400)
            
            return df.lazy().with_columns(
                pl.col(statement_id_col).cast(pl.Utf8)
            )
        except ShapeError as e:
            raise ClientError(f"Could not create dataform due wrong data shape.", 400) from e
        except TypeError as e:
            raise ClientError(f"Wrong type detected in file.", 400) from e
    
    def _get_data(
        self,
        lf: pl.LazyFrame,
        account_name: str,
        *,
        statement_id_col: str = "전표번호",
        account_col: str = "계정과목",
        chabun_col: str = "차변",
        daebun_col: str = "대변",
        summary_col: str = "적요"
    ) -> pl.LazyFrame:
        """Get statement numbers WHERE account_col is account_name.  
        And Get data that WHERE statement_id is in statement_ids."""
        statement_ids = (
            lf.filter(pl.col(account_col) == account_name)
            .select(statement_id_col)
            .unique() # delete overlap ids
        )

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
    ) -> pl.DataFrame:
        """Return collect result."""
        try:
            return raw_data.collect()
        except ColumnNotFoundError as e:
            logger.warning("Could not find column in file. Please check column name.", exc_info=True)
            raise ClientError(f"Could not find column in file. Please check column name.", 400) from e

    def _pivot(
        self,
        data: pl.DataFrame,
        *,
        statement_id_col: str = "전표번호",
        account_col: str = "계정과목",
        chabun_col: str = "차변",
        daebun_col: str = "대변",
        summary_col: str = "적요"
    ) -> pl.DataFrame:
        """Pivot data to object schema and sort it. Then return."""
        chabun_sum = (
            data
            .group_by([statement_id_col, account_col])
            .agg(pl.col(chabun_col).sum().alias("_amount"))
            .with_columns((pl.col(account_col) + pl.lit("(차변)")).alias("_account_type"))
        )

        daebun_sum = (
            data
            .group_by([statement_id_col, account_col])
            .agg(pl.col(daebun_col).sum().alias("_amount"))
            .with_columns((pl.col(account_col) + pl.lit("(대변)")).alias("_account_type"))
        )

        combined_sum = pl.concat([
            chabun_sum.select([statement_id_col, "_account_type", "_amount"]),
            daebun_sum.select([statement_id_col, "_account_type", "_amount"])
        ])

        pivoted = combined_sum.pivot(
            index=statement_id_col,
            on="_account_type",
            values="_amount",
            aggregate_function="sum"
        ).fill_null(0)

        summary_map = data.group_by(statement_id_col).agg(
            pl.col(summary_col).first()
        )

        return (
            pivoted.join(summary_map, on=statement_id_col, how="left")
            .with_columns(pl.lit(None).alias("분류"))
            .select([
                statement_id_col,
                "분류",
                summary_col,
                pl.all().exclude([statement_id_col, "분류", summary_col])
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
    ) -> pl.DataFrame:
        lf = self._read_excel(
            file_ptr,
            statement_id_col=statement_id_col
        )

        raw_data = self._get_data(
            lf,
            account_name,
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

        # Is pivot_data emtpy? -> Raise ClientError
        if pivot_data[statement_id_col] is None:
            logging.error("Failed to analyze data. File is not emtpy, but result is emtpy.")
            raise InternalServerError(f"Failed to analyze data.", 400)

        return pivot_data