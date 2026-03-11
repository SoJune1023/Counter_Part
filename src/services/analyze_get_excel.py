import polars as pl
import logging
logger = logging.getLogger(__name__)

from src.interfaces import BaseAnalyzeExcel

class AnalyzeGetExcel(BaseAnalyzeExcel):
    """
    Example kwargs = {
        "statement_id_col": "전표번호"
        "account_id_col": "계정과목"
        "debit_col": "차변"
        "credit_col": "대변"
        "summary_col": "적요"
    }
    """
    def _get_data(
        self,
        lf: pl.LazyFrame,
        **kwargs
    ) -> pl.LazyFrame:
        """Get all data from df"""
        return (
            lf.select([
                kwargs['statement_id_col'],
                kwargs['account_id_col'],
                kwargs['debit_col'],
                kwargs['credit_col'],
                kwargs['summary_col']
            ])
            .sort(kwargs['statement_id_col'])
        )

    def _pivot(
        self,
        df: pl.DataFrame,
        **kwargs
    ) -> pl.DataFrame:
        """Pivot data to object schema and sort it. Then return."""
        result = df.group_by(kwargs['statement_id_col']).agg([
            pl.col(kwargs['summary_col']).first().alias(kwargs['summary_col']),
            pl.col(kwargs['debit_col']).sum().alias(kwargs['debit_col']),
            pl.col(kwargs['credit_col']).sum().alias(kwargs['credit_col'])
        ])

        return (
            result
            .select([
                kwargs['statement_id_col'],
                kwargs['summary_col'],
                kwargs['debit_col'],
                kwargs['credit_col']
            ])
            .sort(kwargs['statement_id_col'])
            .fill_null(0)
        )