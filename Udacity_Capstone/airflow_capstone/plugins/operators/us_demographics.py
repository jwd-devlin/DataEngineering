from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd
import numpy as np

class USDemographicsOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 data_location ="",
                 table_name ="",
                 data_storage_method="local",
                 data_storage = "",
                 separator = ";",
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
        data_frame = data_frame.replace({pd.np.nan: 0.0})

        del data_frame["Race"]
        del data_frame["Count"]
        data_frame.reset_index(inplace=True)
        return data_frame.drop_duplicates()

    def execute(self, context):

        table_insert = """ INSERT INTO {} VALUES ({}, '{}',
                                                    '{}',
                                                    '{}',
                                                    {},
                                                    {},
                                                    {},
                                                    {},
                                                    {},
                                                    '{}');
        """
        # Import data from local or S3
        us_demo = self.data_storage.import_data_csv(self.data_storage_method, self.data_location, self.separator)

        us_demo_clean = self.__clean_demographics(us_demo)

        # Upload to redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Inserting data into US Demographics table")

        values_insert = """ ({}, '{}',
                                 '{}',
                                '{}',
                                {},
                                {},
                                {},
                                {},
                                {},
                                '{}'),"""
        batch_size = 5
        columns = ["index", "City", "State", "Median Age", "Male Population","Female Population",
                   "Total Population", "Foreign-born",
                   "Average Household Size",
                   "State Code"]

        self.data_storage.upload_data_redshift(redshift, us_demo_clean, batch_size, self.table_name, columns,
                                               values_insert)
