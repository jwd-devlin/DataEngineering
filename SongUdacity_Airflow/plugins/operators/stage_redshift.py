from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook



class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    # Sql copy from S3 commands
    staging_events_copy = ("""
        COPY staging_events FROM '{}'
        credentials 'aws_iam_role={}'
        format as json '{}'
        region '{}';
    """)

    staging_songs_copy = ("""
        COPY staging_songs FROM '{}'
        credentials 'aws_iam_role={}'
        json 'auto'
        region '{}';
    """)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_conn_id="",
                 table = "",
                 s3_path = "",
                 data_format = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.table = table
        self.s3_path = s3_path
        self.data_format = data_format
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):
        aws = AwsHook(self.aws_conn_id)
        credentials = aws.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Copying data from S3 to Redshift")
        # Backfill a specific date
        if self.table == "staging_events":
            formatted_sql = StageToRedshiftOperator.staging_events_copy.format(
                self.s3_path, "arn:aws:iam::036392005668:role/udacity_redshift", self.data_format, "us-west-2"
            )
            redshift.run(formatted_sql)
        elif self.table == "staging_songs":
            formatted_sql = StageToRedshiftOperator.staging_songs_copy.format(
                self.s3_path, "arn:aws:iam::036392005668:role/udacity_redshift", "us-west-2"
            )
            redshift.run(formatted_sql)

        else:
           self.log.info('StageToRedshiftOperator correct sql command not found.')
