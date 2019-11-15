import io
import logging
import numpy as np
import pandas as pd
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
import time

class DataStorage:
    INPUT_DIR = "input"

    def __init__(self):
        self.s3 = S3Hook()
        self.s3_bucket = Variable.get('aws_s3_bucket', "udacity_data")

    def read_input_csv(self, file_path, sep=";"):
        key = f"{self.INPUT_DIR}/{file_path}"
        return self.__read_csv(key, sep)

    def import_data_csv(self, data_storage_method, data_location, separator):
        # Import data from local or S3
        if data_storage_method == "local":
            data = pd.read_csv(data_location, sep=separator)
            return data
        elif data_storage_method == "s3":
            data = self.read_input_csv(data_location, separator)
            return data
        else:
            logging.info('Data Storage location must be set to: local or s3')

    def import_data_sas(self, data_storage_method, data_location, chunk):
        # Import data from local or S3
        if data_storage_method == "local":
            data = pd.read_sas(data_location, 'sas7bdat', encoding="ISO-8859-1", chunksize=chunk, iterator=True)
            return data
        elif data_storage_method == "s3":
            data = pd.read_sas(data_location, 'sas7bdat', encoding="ISO-8859-1", chunksize=chunk, iterator=True)
            return data
        else:
            logging.info('Data Storage location must be set to: local or s3')

    @staticmethod
    def upload_data_redshift(redshift_connect,data_frame,batch_size,table_name, columns,values_insert):


        for df_slice in np.array_split(data_frame, batch_size):
            print("start creating sql sas file")

            start = time.time()
            initial_sql = """ INSERT INTO {} VALUES """.format(table_name)
            for index, row in df_slice.iterrows():
                initial_sql = initial_sql + values_insert.format(*row[columns].tolist())

            print("End creating sql sas file", time.time() - start)
            redshift_connect.run(initial_sql[:-1]+";")

