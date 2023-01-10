from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook

sql_insert = "INSERT INTO {} {}"


class LoadFactOperator(BaseOperator):
    """Airflow operator that loads data into a fact table on AWS Redshift"""

    ui_color = "#F98866"

    @apply_defaults
    def __init__(
        self,
        table: str,
        redshift_conn_id: str,
        sql_select: str,
        *args,
        **kwargs
    ) -> None:

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.sql_select = sql_select
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context) -> None:
        """Loads data into fact table on AWS Redshift"""
        # self.log.info('LoadFactOperator not implemented yet')
        redshift_sql_hook = RedshiftSQLHook(self.redshift_conn_id)
        sql_formatted = sql_insert.format(self.table, self.sql_select)
        redshift_sql_hook.run(sql_formatted)
