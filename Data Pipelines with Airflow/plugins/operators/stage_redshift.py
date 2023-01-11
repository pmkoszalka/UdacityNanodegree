from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.contrib.hooks.aws_hook import AwsHook
import logging

sql_copy = """
    COPY {} FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    region 'us-west-2'
    FORMAT AS JSON '{}';
    """


class StageToRedshiftOperator(BaseOperator):
    """Airflow operator that copies data from s3 to tables within AWS Redshift"""

    ui_color = "#358140"

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id: str,
        aws_credentials_id: str,
        table: str,
        s3_path: str,
        schema_path: str,
        arn: str,
        *args,
        **kwargs,
    ):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.aws_credentials_id = aws_credentials_id
        self.s3_path = s3_path
        self.schema_path = schema_path
        self.arn = arn

    def execute(self, context) -> None:
        """Copies data from s3 to a table in AWS Redshift"""

        logging.info("Logging to AWS and establishing connection with Redshift")
        aws_hook = AwsHook(self.aws_credentials_id)
        redshift_sql_hook = RedshiftSQLHook(self.redshift_conn_id)

        logging.info(f"Copying data from s3 to Redshift table: {self.table}")
        sql_formatted = sql_copy.format(self.table, self.s3_path, self.arn, self.schema_path)
        redshift_sql_hook.run(sql_formatted)
