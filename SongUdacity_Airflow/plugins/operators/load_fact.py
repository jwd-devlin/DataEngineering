from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_name="",
                 sql_command="",
                 reset_collection=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.sql_command = sql_command
        self.reset_collection = reset_collection

    def execute(self, context):
        """
        Loads data from the songs and events table and transforms into the table.
        """
        #self.log.info('LoadFactOperator not implemented yet')

        # Redshift hook: for data storage.
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # if reset is required
        if self.reset_collection:
            self.log.info("Deleting {} table".format(self.table_name))
            redshift.run("DELETE FROM {}".format(self.table_name))

            # Run ETL into the songsfacts table.
        self.log.info("Inserting data into {} table".format(self.table_name))
        redshift.run(self.sql_command)
