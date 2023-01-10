from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook


class DataQualityOperator(BaseOperator):
    """Airflow operator for validation of data in AWS Redshift"""

    ui_color = "#89DA59"

    @apply_defaults
    def __init__(
        self, tables: list, redshift_conn_id: str, *args, **kwargs
    ):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context) -> None:
        """Validates if given table contains rows and if first row contain values; else raises error"""
        redshift_sql_hook = RedshiftSQLHook(self.redshift_conn_id)
        for table in self.tables:
            records = redshift_sql_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if records is None or records[0][0] < 1:
                raise ValueError(f"No records present in destination table - {table}")

            print(f"Data quality for table {table} passed with {records[0][0]} records!")
