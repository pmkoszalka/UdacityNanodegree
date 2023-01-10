from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook

sql_insert = "INSERT INTO {} {}"


class LoadDimensionOperator(BaseOperator):
    """Airflow operator for loading data into dimension table on AWS Redshift"""

    ui_color = "#80BD9E"

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id: str,
        table: str,
        sql_select: str,
        *args,
        **kwargs
    ) -> None:

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_select = sql_select

    def execute(self, context) -> None:
        """Loads data into dimension table on AWS Redshift"""
        redshift_sql_hook = RedshiftSQLHook(self.redshift_conn_id)
        sql_formatted = sql_insert.format(self.table, self.sql_select)
        redshift_sql_hook.run("TRUNCATE TABLE {}".format(self.table))
        redshift_sql_hook.run(sql_formatted)
