from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CreateTablesOperator(BaseOperator):

        @apply_defaults
        def __init__(self,
                     redshift_conn_id="",
                     table_names=[],
                     sql_commands_list=[],
                     reset_collection=False,
                     *args, **kwargs):
            super(CreateTablesOperator, self).__init__(*args, **kwargs)
            self.redshift_conn_id = redshift_conn_id
            self.table_names = table_names
            self.sql_commands_list = sql_commands_list
            self.reset_collection = reset_collection

        def execute(self, context):

            # Redshift hook: for data storage.
            redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

            tables_count = len(self.sql_commands_list)

            for table in range(tables_count):
                # if reset is required
                if self.reset_collection:
                    self.log.info("Deleting {} table".format(self.table_names[table]))
                    redshift.run("DROP TABLE IF EXISTS {}".format(self.table_names[table]))

                self.log.info("Inserting data into {} table".format(self.table_names[table]))
                redshift.run(self.sql_commands_list[table])
