import polars as pl
import logging
logger = logging.getLogger(__name__)

from src.interfaces import BaseAnalyzeExcel

class AnalyzeAccountTable(BaseAnalyzeExcel):
    """
    Example kwargs = {
        "selected_account_id": <선택한 계정 과목>
        "classification_col": "분류"
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
        """Get statement numbers WHERE account_col is account_name.  
        And Get data that WHERE statement_id is in statement_ids."""
        statement_ids = (
            lf.filter(pl.col(kwargs['account_id_col']) == kwargs['selected_account_id'])
            .select(kwargs['statement_id_col'])
            .unique() # delete overlap ids
        )

        return (
            lf.join(statement_ids, on=kwargs['statement_id_col'], how="semi")
            .select([
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
        chabun_sum = (
            df
            .group_by([kwargs['statement_id_col'], kwargs['account_id_col']])
            .agg(pl.col(kwargs['debit_col']).sum().alias("_amount"))
            .with_columns((pl.col(kwargs['account_id_col']) + pl.lit("(차변)")).alias("_account_type"))
        )

        daebun_sum = (
            df
            .group_by([kwargs['statement_id_col'], kwargs['account_id_col']])
            .agg(pl.col(kwargs['credit_col']).sum().alias("_amount"))
            .with_columns((pl.col(kwargs['account_id_col']) + pl.lit("(대변)")).alias("_account_type"))
        )

        combined_sum = pl.concat([
            chabun_sum.select([kwargs['statement_id_col'], "_account_type", "_amount"]),
            daebun_sum.select([kwargs['statement_id_col'], "_account_type", "_amount"])
        ])

        pivoted = combined_sum.pivot(
            index=kwargs['statement_id_col'],
            on="_account_type",
            values="_amount",
            aggregate_function="sum"
        ).fill_null(0)

        summary_map = df.group_by(kwargs['statement_id_col']).agg(
            pl.col(kwargs['summary_col']).first()
        )

        return (
            pivoted.join(summary_map, on=kwargs['statement_id_col'], how="left")
            .with_columns(pl.lit(None).alias("분류"))
            .select([
                kwargs['statement_id_col'],
                kwargs['classification_col'],
                kwargs['summary_col'],
                pl.all().exclude([kwargs['statement_id_col'], "분류", kwargs['summary_col']])
            ])
            .sort(kwargs['statement_id_col'])
        )
