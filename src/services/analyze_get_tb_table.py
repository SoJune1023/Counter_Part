import polars as pl
import logging
logger = logging.getLogger(__name__)

from typing import IO
from polars.exceptions import ShapeError, ColumnNotFoundError

from src.exceptions import ClientError, InternalServerError

class AnalyzeGetTbTable:
    def _read_excel(
        self,
        file_ptr: IO[bytes],
        *,
        statement_id_col: str = "전표번호",
    ) -> pl.LazyFrame:
        """Read excel file"""
        try:
            df = pl.read_excel(file_ptr, engine="calamine")

            if df.is_empty():
                logger.warning("There is nothing to get in file.")
                raise ClientError(f"There is no data to get.", 400)
            
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
        *,
        account_col: str = "계정과목",
        chabun_col: str = "차변",
        daebun_col: str = "대변",
    ) -> pl.LazyFrame:
        """Get all data from df"""
        return (
            lf.select([
                account_col,
                chabun_col,
                daebun_col,
            ])
            .sort(account_col)
        )
    
    def _collect(
        self,
        lf: pl.LazyFrame
    ) -> pl.DataFrame:
        """Return collect result"""
        try:
            return lf.collect()
        except ColumnNotFoundError as e:
            logger.warning("Could not find column in file. Please check column name.", exc_info=True)
            raise ClientError(f"Could not find column in file. Please check column name.", 400) from e
    
    def _pivot(
        self,
        data: pl.DataFrame,
        *,
        account_col: str = "계정과목",
        chabun_col: str = "차변",
        daebun_col: str = "대변",
    ) -> pl.DataFrame:
        account_chabun_sum = (
            data
            .group_by([account_col])
            .agg(pl.col(chabun_col).sum().alias("_amount"))
            .with_columns(pl.lit(chabun_col).alias("_type"))
        )

        account_daebun_sum = (
            data
            .group_by([account_col])
            .agg(pl.col(daebun_col).sum().alias("_amount"))
            .with_columns(pl.lit(daebun_col).alias("_type"))
        )

        combined_sum = pl.concat([
            account_chabun_sum.select([account_col, "_type", "_amount"]),
            account_daebun_sum.select([account_col, "_type", "_amount"])
        ])

        pivoted = combined_sum.pivot(
            index=account_col,
            on="_type",
            values="_amount",
            aggregate_function="sum"
        ).fill_null(0)

        return (
            pivoted
            .sort(account_col)
        )
    
    def process(
        self,
        file_ptr: IO[bytes],
        *,
        statement_id_col: str = "전표번호",
        account_col: str = "계정과목",
        chabun_col: str = "차변",
        daebun_col: str = "대변",
        summary_col: str = "적요"
    ) -> pl.DataFrame:
        """Process analyze get excel logic"""
        lf = self._read_excel(
            file_ptr,
            statement_id_col=statement_id_col
        )

        raw_data = self._get_data(
            lf,
            account_col=account_col,
            chabun_col=chabun_col,
            daebun_col=daebun_col,
        )

        collected_data = self._collect(raw_data)

        pivot_data = self._pivot(
            collected_data,
            account_col=account_col,
            chabun_col=chabun_col,
            daebun_col=daebun_col,
        )

        if pivot_data[account_col] is None:
            logging.error("Failed to load data. File is not emtpy, but result is emtpy.")
            raise InternalServerError(f"Failed to load data.", 400)
        
        return pivot_data