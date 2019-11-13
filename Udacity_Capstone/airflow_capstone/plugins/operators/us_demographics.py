from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd

class USDemographicsOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 data_location ="",
                 table_name ="",
                 data_storage_method="local",
                 data_storage = "",
                 separator = "",
                 *args, **kwargs):
        super(USDemographicsOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.data_location = data_location
        self.data_storage_method = data_storage_method
        self.table_name = table_name
        self.data_storage = data_storage
        self.separator = separator

    @staticmethod
    def __clean_demographics(data_frame):
        data_frame["City"] = data_frame["City"].str.replace("'","''")
        return data_frame

    def execute(self, context):

        table_insert = """ INSERT INTO {} VALUES ({}, '{}',
                                                    '{}',
                                                    '{}',
                                                    {},
                                                    {},
                                                    {},
                                                    {},
                                                    {},
                                                    '{}',
                                                    '{}',
                                                    '{}');
        """
        # Import data from local or S3
        us_demo = self.data_storage.import_data_csv(self.data_storage_method, self.data_location, self.separator)

        us_demo_clean = self.__clean_demographics(us_demo)

        # Upload to redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Inserting data into US Demographics table")

        for index, row in us_demo_clean.iterrows():
            sql_cmd = table_insert.format(self.table_name,
                                          index,
                                        row["City"],
                                        row["State"],
                                        row["Median Age"],
                                        row["Male Population"],
                                        row["Female Population"],
                                        row["Total Population"],
                                        row["Foreign-born"],
                                        row["Average Household Size"],
                                        row["State Code"],
                                        row["Race"],
                                        row["Count"])
            redshift.run(sql_cmd)
