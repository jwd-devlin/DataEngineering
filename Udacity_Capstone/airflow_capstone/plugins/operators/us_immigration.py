
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd
import numpy as np

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
                 additional_data = "",
                 *args, **kwargs):
        super(USImmigrationOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.data_location = data_location
        self.data_storage = data_storage
        self.table_name = table_name
        self.data_storage_method = data_storage_method
        self.additional_data = additional_data


    @staticmethod
    def __delete_columns(data_frame,delete_list):
        for col in delete_list:
            del data_frame[col]

    def __clean_immigration(self,data_frame):
        """
        keep:
            - i94port:  character code of destination USA city, codes america only for mapping to gps coordinates.
            - biryear: birth year, General interest.
            - i94cit: origin of traveler 3 digit code , General interest.
            - depdate: departure date from the USA, General interest.
            - i94visa: reason for immigration, General interest.
            - i94mon: month, General interest.
            - i94yr:   year, General interest.
        """
        key_data = ["i94port","biryear","i94cit","depdate","i94visa","i94mon","i94yr","i94mode"]
        delete = [col for col in data_frame.columns.tolist() if col not in key_data]

        self.__delete_columns(data_frame, delete)

        # invalid/Foreign ports drop
        data_frame = self.additional_data.remove_invalid_ports(data_frame, port_code = "i94port")

        # TODO: Add Assuming that air travel is the only important values (i94mode:1) for air
        data_frame = data_frame[data_frame["i94mode"] == 1.0]

        data_frame.reset_index(inplace = True)
        data_frame = data_frame.replace({pd.np.nan: 0.0})
        return data_frame

    def execute(self, context):
        # Redshift hook: for data storage.
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        year = ["jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"]

        for month in year:
            file_location = self.data_location + "i94_{}16_sub.sas7bdat".format(month)

            immigration_month = self.data_storage.import_data_sas(self.data_storage_method, file_location, 75000)

            for df_month_chunk in immigration_month:
                df_clean = self.__clean_immigration(df_month_chunk)
                print(month, df_clean.shape)
                values_insert = """ ('{}', {}, '{}', '{}', '{}', {}, {}, '{}'),"""
                batch_size = 5
                columns = ["i94port", "biryear", "i94cit", "depdate", "i94visa", "i94mon", "i94yr",
                           "port_names"]
                table_name_plus = self.table_name +"("+",".join(columns)+")"
                self.data_storage.upload_data_redshift(redshift, df_clean, batch_size, table_name_plus, columns,
                                                       values_insert)
