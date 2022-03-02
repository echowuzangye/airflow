from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_statement="",
                 append_data="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_statement = sql_statement
        self.append_data = append_data

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.append_data:
            self.log.info(f'LoadFactOperator: INSERT data into dimension table {self.table}')
            insert_statement = f"INSERT INTO {self.table} {self.sql_statement}"
            redshift.run(insert_statement)
        else:
            self.log.info(f'LoadFactOperator: Delete data from dimension table {self.table}')
            redshift.run(f"DELETE FROM {self.table}")
            
            
            
