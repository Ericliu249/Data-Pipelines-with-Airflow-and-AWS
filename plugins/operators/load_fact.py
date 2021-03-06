from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    insert_sql = """
        INSERT INTO {}
        {};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 insert_sql='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insert_sql = insert_sql

    def execute(self, context):
        self.log.info(f'Load data into fact table {self.table} is now in progress')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"Inserting data into fact table {self.table}")
        formatted_sql = LoadFactOperator.insert_sql.format(
            self.table,
            self.insert_sql
        )
        redshift.run(formatted_sql)
        self.log.info(f'Load Fact table {self.table} is now completed')