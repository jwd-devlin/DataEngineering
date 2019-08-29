import configparser
from datetime import datetime
import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql import types as t
import pyspark.sql.functions as F
from pyspark.sql.functions import monotonically_increasing_id

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # data locations
    song_data_location = input_data +  "song_data/*/*/*/*.json"
    # output locations
    songs_table_output_path = output_data + "songs_table"
    artists_table_output_path = output_data + "artists_table"
    # get filepath to song data file
    song_data = spark.read.json(song_data_location)

    # read song data file
    print(song_data.printSchema())

    # extract columns to create songs table
    songs_table = song_data.select("song_id", "title", "artist_id", "year", "duration").distinct().orderBy(["song_id"], ascending=False)

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id")\
        .parquet(songs_table_output_path)

    # extract columns to create artists table
    artists_table = song_data.select("artist_id", "artist_name", "artist_latitude", "artist_longitude", "artist_location").distinct().orderBy(["artist_id"], ascending=False)

    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(artists_table_output_path)

 def process_log_data(spark, input_data, output_data):
    #  get filepath to log data file
    log_input_data = input_data + "log_data/*/*/*.json"

    # output locations
    users_table_output_path = output_data + "users_table"

    time_table_output_path = output_data +"time_table"

    songplays_table_output_path = output_data +"songplays_table"
    # read log data file
    log_data = spark.read.json(log_input_data)

    # filter by actions for song plays
    log_data_filtered = log_data.filter((col("page") == 'NextSong'))

    # extract columns for users table
    users_table = log_data.select("firstName", "lastName", "gender", "level", "userId").distinct()

    # write users table to parquet files
    users_table.mode("overwrite").parquet(users_table_output_path)

    # create timestamp column from original timestamp column
    @udf(t.TimestampType())
    def get_timestamp(ts):
        return datetime.datetime.fromtimestamp(ts / 1000)

    # create datetime column from original timestamp column **Not needed.
    #get_datetime = udf()
    #df =

    # extract columns to create time table
    time_table = log_data.select("ts").distinct()
    time_table = time_table.withColumn("timestamp", get_timestamp(time_table.ts))
    # convert time
    convert_time = {"hour":F.hour("timestamp"),"day": F.dayofweek("timestamp"),"week":F.weekofyear("timestamp"),
                   "month": F.month("timestamp"),"year": F.year("timestamp"),"weekday": F.dayofweek("timestamp")}
    for i in convert_time.keys():
       time_table = time_table.withColumn(i, convert_time[i])

    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(time_table_output_path)

    # songs location
    song_data_location = input_data + "song_data/*/*/*/*.json"
    # read in song data to use for songplays table
    song_df = spark.read.json(song_data_location)

    # extract columns from joined song and log datasets to create songplays table
    songs_select = song_df.select("song_id", "artist_id","artist_name","title")
    log_select_filtered = log_data_filtered.select("ts", "artist", "userId", "sessionId", "location", "song",
                                                   "userAgent", "level")
    log_song_joined = log_select_filtered \
        .join(songs_select, (log_select_filtered.artist == songs_select.artist_name) & \
              (log_select_filtered.song == songs_select.title))
    # write songplays table to parquet files partitioned by year and month
    songplays_table = log_song_joined.select("ts","artist_id","song_id","location","userId","userAgent","level")

    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id())

    songplays_table = songplays_table.join(time_table.select("ts","year","month"), (time_table.ts == songplays_table.ts))
    songplays_table.write.mode("overwrite").partitionBy("year", "month") \
        .parquet(songplays_table_output_path)

def main():
    spark = create_spark_session()
    input_data = config['S3']['SOURCE_S3']
    output_data = config['S3']['OUTPUT_S3']
    
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)



if __name__ == "__main__":
    main()
