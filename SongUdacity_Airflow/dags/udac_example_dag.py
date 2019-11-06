from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator, CreateTablesOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
# export AIRFLOW_HOME="$(pwd)"
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False,
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_in_redshift = CreateTablesOperator(task_id= 'Create_tables',
                                                 dag= dag,
                                                 redshift_conn_id="redshift",
                                                 tables_count=7,
                                                 table_names=["staging_events", "staging_songs", "songplay", "users",
                                                               "artists", "songs", "time"],
                                                 sql_commands_list=[SqlQueries.staging_events_table_create,
                                                                     SqlQueries.staging_songs_table_create,
                                                                     SqlQueries.songplay_table_create,
                                                                     SqlQueries.user_table_create,
                                                                     SqlQueries.artist_table_create,
                                                                     SqlQueries.song_table_create,
                                                                     SqlQueries.time_table_create],
                                                 reset_collection=True,
                                                 )
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    provide_context=False,
    dag=dag,
    table = "staging_events",
    s3_path = "s3://udacity-dend/log_data",
    redshift_conn_id="redshift",
    aws_conn_id="aws_credentials",
    data_format="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    provide_context=False,
    dag=dag,
    table = "staging_songs",
    s3_path = "s3://udacity-dend/song_data",
    redshift_conn_id="redshift",
    aws_conn_id="aws_credentials",
)


load_songplays_table = LoadFactOperator(
     task_id='Load_songplays_fact_table',
     dag=dag,
     redshift_conn_id="redshift",
     table_name="songplay",
     sql_command=SqlQueries.songplay_table_insert,
     reset_collection=False
 )

load_user_dimension_table = LoadDimensionOperator(
     task_id='Load_user_dim_table',
     dag=dag,
     redshift_conn_id="redshift",
     table_name="users",
     sql_command=SqlQueries.user_table_insert,
     reset_collection=False
 )

load_song_dimension_table = LoadDimensionOperator(
     task_id='Load_song_dim_table',
     dag=dag,
     redshift_conn_id="redshift",
     table_name="songs",
     sql_command=SqlQueries.song_table_insert,
     reset_collection=False
 )

load_artist_dimension_table = LoadDimensionOperator(
     task_id='Load_artist_dim_table',
     dag=dag,
     redshift_conn_id="redshift",
     table_name="artists",
     sql_command=SqlQueries.artist_table_insert,
     reset_collection=False
 )

load_time_dimension_table = LoadDimensionOperator(
     task_id='Load_time_dim_table',
     dag=dag,
     redshift_conn_id="redshift",
     table_name="artists",
     sql_command=SqlQueries.time_table_insert,
     reset_collection=False
 )

run_quality_checks = DataQualityOperator(task_id='Run_data_quality_checks',
                                         dag=dag,
                                         table_names = ["staging_events", "staging_songs", "songplay", "users",
                                                               "artists", "songs", "time"],
                                         redshift_conn_id="redshift")

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables_in_redshift

create_tables_in_redshift >> [stage_events_to_redshift, stage_songs_to_redshift]

[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table


load_songplays_table >> [load_user_dimension_table ,load_time_dimension_table, load_song_dimension_table,load_artist_dimension_table ]

[load_user_dimension_table,  load_time_dimension_table, load_song_dimension_table, load_artist_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator