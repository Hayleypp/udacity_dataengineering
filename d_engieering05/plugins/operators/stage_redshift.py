from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self, redshift_conn_id="", aws_credentials="", table="", s3_bucket="", s3_key="", json_path="auto", region="us-west-2", *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.region = region
        
    def execute(self, context):
        self.s3 =S3Hook(aws_conn_id="aws_credentials")
        credentials = self.s3.get_credentials()
        redshift = PostgresHook(self.redshift_conn_id)
        #sql format
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        sql_format = """COPY public.{} FROM '{}' ACCESS_KEY_ID '{}' SECRET_ACCESS_KEY '{}' REGION '{}' JSON '{}'"""
        #load
        sql = sql_format.format(self.table, s3_path, credentials.access_key, credentials.secret_key, self.region, self.json_path)
        redshift.run(sql)




