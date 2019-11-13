from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import CreateTablesOperator, USCityCoordinatesOperator, USDemographicsOperator, USAirportsOperator
from helpers import SqlQueries, DataStorage, DataLocations

"""
For local data storage:
   - Add local locations in to data_locations.py
   - set data_storage_method: "local"

For s3 data Storage:
    - Add s3 credentials to airflow.
    - Add bucket name in DataStorage (replace :'udacity')
    - Add local locations in to data_locations.py
    - set data_storage_method: "s3"

"""


default_args = {
    'owner': 'Jdev',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'catchup_by_default': False,
    'email_on_retry': False
}

dag = DAG('us_cities_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_in_redshift = CreateTablesOperator(task_id= 'Create_tables',
                                                 dag= dag,
                                                 redshift_conn_id="redshift",
                                                 tables_count=4,
                                                 table_names=["states","cities","demographics","airports"],
                                                 sql_commands_list=[SqlQueries.us_states_table,
                                                                    SqlQueries.us_city_table,
                                                                    SqlQueries.us_city_demographics,
                                                                    SqlQueries.us_airports],
                                                 reset_collection=True,
                                                 )
load_city_tables = USCityCoordinatesOperator(
        task_id='load_city_stat_info',
        dag=dag,
        redshift_conn_id="redshift"
 )

load_demographics = USDemographicsOperator(
        task_id='load_demographics_info',
        dag=dag,
        redshift_conn_id="redshift",
        data_location = DataLocations.us_demographics_data,
        data_storage_method = "local",
        table_name = "demographics",
        data_storage = DataStorage,
        separator = ";",
)

load_airports = USAirportsOperator(
        task_id='load_airports_info',
        dag=dag,
        redshift_conn_id="redshift",
        data_location = DataLocations.us_airports_data,
        data_storage_method = "local",
        table_name = "airports",
        data_storage = DataStorage,
        separator = ",",
)

#load_immigration =



start_operator >> create_tables_in_redshift >> [load_demographics ,load_airports]