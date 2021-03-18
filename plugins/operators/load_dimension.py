from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    insert_sql = """
        INSERT INTO {}
        {};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 insert_sql='',
                 delete=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insert_sql = insert_sql
        self.delete=delete

    def execute(self, context):       
        self.log.info(f'Load data into dimension table {self.table} is now in progress')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.delete:
            self.log.info(f"Truncating Redshift dimension table {self.table}")
            redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info(f"Inserting data into dimension table {self.table}")
        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table,
            self.insert_sql
        )
        redshift.run(formatted_sql)
        self.log.info(f'Load Dimension table {self.table} is now completed')
