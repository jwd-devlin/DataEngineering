import io
import logging

import pandas as pd
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable


class DataStorage:
    INPUT_DIR = "input"
    OUTPUT_DIR = "output"
    REFERENCE_DIR = "reference"

    def __init__(self):
        self.s3 = S3Hook()
        self.s3_bucket = Variable.get('aws_s3_bucket', "udacity_data")

    def read_input_csv(self, file_path, sep=";"):
        key = f"{self.INPUT_DIR}/{file_path}"
        return self.__read_csv(key, sep)

    def read_reference_csv(self, file_path, sep=","):
        key = f"{self.REFERENCE_DIR}/{file_path}"
        return self.__read_csv(key, sep)

    def upload_output(self, file_path, data):
        key = f"{self.OUTPUT_DIR}/{file_path}"
        logging.info('Uploading: ' + key)
        self.s3.load_string(data, key, bucket_name=self.s3_bucket)

    def upload_input(self, file_path, bytes_data):
        key = f"{self.INPUT_DIR}/{file_path}"
        logging.info('Uploading: ' + key)
        self.s3.load_bytes(bytes_data, key, bucket_name=self.s3_bucket)

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
