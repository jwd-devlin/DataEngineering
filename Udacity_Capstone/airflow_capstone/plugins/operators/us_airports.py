from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd

class USAirportsOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 data_location ="",
                 table_name ="",
                 data_storage="",
                 data_storage_method = "local",
                 separator = ",",
                 *args, **kwargs):
        super(USAirportsOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.data_location = data_location
        self.data_storage = data_storage
        self.table_name = table_name
        self.data_storage_method = data_storage_method
        self.separator = separator

    @staticmethod
    def __data_clean_airports( data_frame):
        us_airports = data_frame[data_frame["iso_country"] == "US"]

        # split coordinates :LATITUDE,LONGITUDE
        us_airports.loc[:, "LATITUDE"] = us_airports["coordinates"].apply(lambda x: x.split(",")[1])
        us_airports.loc[:, "LONGITUDE"] = us_airports["coordinates"].apply(lambda x: x.split(",")[0])

        #fix names
        us_airports["name"] = us_airports["name"].str.replace("'", "''")
        us_airports["municipality"] = us_airports["municipality"].str.replace("'", "''")

        us_airports.reset_index(inplace=True)
        return us_airports



    def execute(self, context):

        # Import data from local or S3
        us_airports = self.data_storage.import_data_csv(self.data_storage_method, self.data_location, self.separator)

        # Clean data
        clean_data = self.__data_clean_airports(us_airports)

        # Upload to redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Inserting data into US Demographics table")


        values_insert = """ ({}, '{}',
                                                    '{}',
                                                    '{}',
                                                    '{}',
                                                    '{}',
                                                    '{}',
                                                    {},
                                                    {}),"""
        batch_size = 5
        columns = ["index","ident","name","elevation_ft","iso_region","municipality","coordinates","LATITUDE","LONGITUDE"]


        self.data_storage.upload_data_redshift(redshift,clean_data,batch_size,self.table_name, columns,values_insert)
