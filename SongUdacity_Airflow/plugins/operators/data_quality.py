from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    sql_count_entries = """SELECT COUNT(*) FROM {}"""

    @apply_defaults
    def __init__(self,
                 table_names=[],
                 redshift_conn_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table_names = table_names
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('DataQualityOperator checking for empty tables')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        #Check for empty tables.
        DataQualityOperator.not_empty_tables(self, self.table_names, redshift)

    def not_empty_tables(self,tables_names, redshift):
        """
        Provided a list of the table names and assessment will be made to de
        """
        def check_empty_results(entry):
            if not entries:
                return True
            elif len(entries[0]) < 1:
                return True
            else:
                return False

        for table in tables_names:
            sql_command = DataQualityOperator.sql_count_entries.format(table)
            entries = redshift.get_records(sql_command)
            #self.log.info('DataQualityOperator entry {} for table {}'.format(entries, table))
            if check_empty_results(entries):
                self.log.info('DataQualityOperator check for  {} table failed :('.format(table))
            elif entries[0][0] == 0 :
                self.log.info('DataQualityOperator found zero ( {} ) results for  {} table. Nothing imported. :('.format(entries[0][0]
                                                                                                             , table))
            else:
                self.log.info('DataQualityOperator found {} entry results for  {} table. Success !!'.format(entries[0][0]
                                                                                                          , table))
