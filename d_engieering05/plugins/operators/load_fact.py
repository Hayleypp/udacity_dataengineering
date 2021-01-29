from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,table="", redshift_conn_id="", sql="", *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.sql=sql

    def execute(self, context):
        redshift=PostgresHook(self.redshift_conn_id)
        sql_stm="""INSERT INTO public.{} {}"""
        sql_format=sql_stm.format(self.table, self.sql)
        redshift.run(sql_format)
        self.log.info('Load fact table')
