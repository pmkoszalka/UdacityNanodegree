from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
import logging


class DataQualityOperator(BaseOperator):
    """Airflow operator for validation of data in AWS Redshift"""

    ui_color = "#89DA59"

    @apply_defaults
    def __init__(self, tests: list, redshift_conn_id: str, *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tests = tests
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context) -> None:
        """Validates if given table contains rows and if first row contain values; else raises error"""
        
        logging.info("Establishing connection with Redshift")
        redshift_sql_hook = RedshiftSQLHook(self.redshift_conn_id)

        logging.info(f"Data quality check is initialized!")
        for i, test in enumerate(self.tests):
            records = redshift_sql_hook.get_records(test["quality_test"]
            )

            logging.info(f"Running query: {test['quality_test']}")
            logging.info(f"Unwanted result: {test['unwanted_result']}")
            logging.info(f"Result: {records[0][0]}")

            if records[0][0] == test["unwanted_result"]:
                raise ValueError(f"Data quality check number {i} has failed!")
            logging.info(f"Data quality check number {i} has passed!")
