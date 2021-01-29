from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self, redshift_conn_id="", table="", sql="", append = False,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.append = append
       

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        if not self.append:
            self.log.info(f"Delete {self.table} table")
            sqlstm="""TRUNCATE {}"""
            sqlformat=sqlstm.format(self.table)
            redshift.run(sqlformat)
            
        sql_stm="""INSERT INTO public.{} {}"""
        sql_format=sql_stm.format(self.table, self.sql)
        self.log.info(f'Load data into {self.table} dimension table')
        redshift.run(sql_format) 
        
