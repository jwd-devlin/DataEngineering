
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd

class USImmigrationOperator(BaseOperator):
    """

    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 data_location="",
                 table_name="",
                 data_storage="",
                 data_storage_method="local",
                 *args, **kwargs):
        super(USImmigrationOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.data_location = data_location
        self.data_storage = data_storage
        self.table_name = table_name
        self.data_storage_method = data_storage_method



    def execute(self, context):
        # Redshift hook: for data storage.
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)



