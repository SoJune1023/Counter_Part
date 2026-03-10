import polars as pl
import logging
logger = logging.getLogger(__name__)

from src.interfaces import BaseAnalyzeExcel

class AnalyzeGetTbTable(BaseAnalyzeExcel):
    def _get_data(
        self,
        lf: pl.LazyFrame,
        **kwargs
    ) -> pl.LazyFrame:
        """Get all data from df"""
        return (
            lf.select([
                kwargs['account_id_col'],
                kwargs['debit_col'],
                kwargs['credit_col']
            ])
            .sort(kwargs['account_id_col'])
        )

    def _pivot(
        self,
        data: pl.DataFrame,
        **kwargs
    ) -> pl.DataFrame:
        account_chabun_sum = (
            data
            .group_by([kwargs['account_id_col']])
            .agg(pl.col(kwargs['debit_col']).sum().alias("_amount"))
            .with_columns(pl.lit(kwargs['debit_col']).alias("_type"))
        )

        account_daebun_sum = (
            data
            .group_by([kwargs['account_id_col']])
            .agg(pl.col(kwargs['credit_col']).sum().alias("_amount"))
            .with_columns(pl.lit(kwargs['credit_col']).alias("_type"))
        )

        combined_sum = pl.concat([
            account_chabun_sum.select([kwargs['account_id_col'], "_type", "_amount"]),
            account_daebun_sum.select([kwargs['account_id_col'], "_type", "_amount"])
        ])

        pivoted = combined_sum.pivot(
            index=kwargs['account_id_col'],
            on="_type",
            values="_amount",
            aggregate_function="sum"
        ).fill_null(0)

        return (
            pivoted
            .sort(kwargs['account_id_col'])
        )
