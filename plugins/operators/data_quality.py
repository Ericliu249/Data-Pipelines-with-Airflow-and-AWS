from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        self.log.info('Data quality check is now in progress')        
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table_column in self.tables:
                table = table_column[0]
                column = table_column[1]
                records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
                if len(records) < 1 or len(records[0]) < 1:
                    raise ValueError(f"Data quality check failed. {table} returned no results")
                num_records = records[0][0]
                if num_records < 1:
                    raise ValueError(f"Data quality check failed. {table} contained 0 rows")
                null_records = redshift_hook.get_records(f"SELECT COUNT({column}) FROM {table} WHERE {column} IS NULL")
                self.log.info("null_records: ", null_records)
                self.log.info("null_records[0]: ", null_records[0])
                if null_records[0][0] > 0:
                    raise ValueError(f"Data quality check failed. {table} contained null values on column {column}")
                self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")
