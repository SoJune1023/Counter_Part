import polars as pl
import logging
logger = logging.getLogger(__name__)

from typing import IO
from abc import ABC, abstractmethod
from polars.exceptions import ShapeError, ColumnNotFoundError

from src.exceptions import ClientError

class BaseAnalyzeExcel(ABC):
    """
    Example kwargs = {
        "selected_account_id": <선택한 계정 과목> | Option
        "classification_col": "분류" | Option
        "statement_id_col": "전표번호"
        "account_id_col": "계정과목"
        "debit_col": "차변"
        "credit_col": "대변"
        "summary_col": "적요"
    }
    """
    def _read_excel(
        self,
        file_ptr: IO[bytes],
        **kwargs
    ) -> pl.LazyFrame:
        """
        Read excel file from file_ptr, and sort by statement_id than return it.

        :param file_ptr: Excel file ptr.
        :type  file_ptr: IO[bytes]
        :raise ClientError: Wrong excel data.
        """
        try:
            df = pl.read_excel(file_ptr, engine="calamine")

            if df.is_empty():
                logger.warning("There is nothing to get in file.")
                raise ClientError(f"There is no data to get.", 400)

            return df.lazy().with_columns(
                pl.col(kwargs['statement_id_col']).cast(pl.Utf8)
            )
        except ShapeError as e:
            raise ClientError(f"Could not create dataform due wrong data shape.", 400) from e
        except TypeError as e:
            raise ClientError(f"Wrong type detected in file.", 400) from e
        
    @abstractmethod
    def _get_data(
        self,
        lf: pl.LazyFrame,
        **kwargs
    ) -> pl.LazyFrame:
        pass

    def _collect(self, lf: pl.LazyFrame) -> pl.DataFrame:
        """
        Return collect result.
        
        :param lf: LazyFrame of excel data.
        :type  lf: LazyFrame

        :return: Collected data.

        :raise ClientError: Failed to find column in file.
        """
        try:
            return lf.collect()
        except ColumnNotFoundError as e:
            logger.warning("Could not find column in file. Please check column name.", exc_info=True)
            raise ClientError(f"Could not find column in file. Please check column name.", 400) from e
    
    @abstractmethod
    def _pivot(
        self,
        df: pl.DataFrame,
        **kwargs
    ) -> pl.DataFrame:
        pass

    def process(
        self,
        file_ptr: IO[bytes],
        **kwargs
    ) -> pl.DataFrame:
        lf = self._read_excel(file_ptr, **kwargs)

        raw_data = self._get_data(lf, **kwargs)

        collected_data = self._collect(raw_data)

        pivoted_data = self._pivot(collected_data, **kwargs)

        return pivoted_data
