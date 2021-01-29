from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self, redshift_conn_id='', tables=[], *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tables=tables
        self.redshift_conn_id=redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        for table in self.tables:
            self.log.info(f"Data Quality Check for {table} table")
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"{table} returns no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"{table} has 0 row")
            self.log.info(f"Data quality check on {table} passed with {records[0][0]}")