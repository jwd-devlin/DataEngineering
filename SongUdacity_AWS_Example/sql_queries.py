import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA  = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
LOCATION  = config.get('AWS','LOCATION')
IAM_ROLE_NAME   = config.get('IAM_ROLE','ROLE_NAME')
DWH_ROLE_ARN = config.get('IAM_ROLE','DWH_ROLE_ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
                     artist VARCHAR,
                       auth VARCHAR,
                  firstName VARCHAR,
                     gender VARCHAR,
              iteminsession INTEGER,
                   lastname VARCHAR,
                     length FLOAT,
                      level VARCHAR,
                   location VARCHAR,
                     method VARCHAR,
                       page VARCHAR,
               registration FLOAT,
                  sessionid INTEGER,
                       song VARCHAR,
                     status INTEGER,
                         ts BIGINT,
                  useragent VARCHAR,
                     userid INTEGER
);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
                num_songs           INTEGER         NULL,
                artist_id           VARCHAR         NOT NULL,
                artist_latitude     VARCHAR         NULL,
                artist_longitude    VARCHAR         NULL,
                artist_location     VARCHAR         NULL,
                artist_name         VARCHAR         NULL,
                song_id             VARCHAR         NOT NULL,
                title               VARCHAR         NULL,
                duration            DECIMAL         NULL,
                year                INTEGER         NULL
    );
""")

songplay_table_create = ("""
                    DROP TABLE IF EXISTS songplays;
                    CREATE TABLE IF NOT EXISTS songplays (
                        songplay_id INTEGER IDENTITY(1,1) PRIMARY KEY SORTKEY, 
                         start_time BIGINT  NOT NULL, 
                            user_id INTEGER, 
                              level VARCHAR, 
                            song_id VARCHAR, 
                          artist_id VARCHAR, 
                         session_id INTEGER, 
                           location VARCHAR NOT NULL, 
                         user_agent VARCHAR NOT NULL);
""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS users (
                        user_id INTEGER PRIMARY KEY, 
                     first_name VARCHAR NOT NULL,
                      last_name VARCHAR NOT NULL,
                         gender VARCHAR, 
                          level VARCHAR)
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs ( 
                        song_id VARCHAR PRIMARY KEY, 
                          title VARCHAR NOT NULL, 
                      artist_id VARCHAR NOT NULL, 
                           year INTEGER, 
                       duration DECIMAL)
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                     artist_id VARCHAR PRIMARY KEY, 
                          name VARCHAR NOT NULL, 
                      location VARCHAR, 
                     lattitude VARCHAR, 
                     longitude VARCHAR)
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                 start_time varchar(50) PRIMARY KEY, 
                      hour INTEGER, 
                       day INTEGER, 
                      week INTEGER, 
                     month INTEGER, 
                      year INTEGER,
                   weekday INTEGER)
""")


# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    credentials 'aws_iam_role={}'
    format as json {}
    region '{}';
""").format(LOG_DATA, DWH_ROLE_ARN, LOG_JSONPATH,LOCATION)



staging_songs_copy = ("""
    COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
    json 'auto'
    region '{}';
""").format(SONG_DATA, DWH_ROLE_ARN,LOCATION)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (             start_time,
                                        user_id,
                                        level,
                                        song_id,
                                        artist_id,
                                        session_id,
                                        location,
                                        user_agent)
    SELECT   e.ts   AS start_time,
             e.userid AS user_id,
             e.level as level,
             s.song_id as song_id,
             s.artist_id AS artist_id,
             e.sessionid as session_id,
             e.location AS location,
             e.useragent AS useragent
    FROM staging_events AS e
    JOIN staging_songs AS s
    ON   (e.artist = s.artist_name)
    WHERE e.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users  ( user_id , 
                      first_name ,
                       last_name ,
                          gender , 
                          level)
    SELECT DISTINCT e.userid AS user_id,
           e.firstName AS first_name,
           e.lastname AS last_name, 
           e.gender AS gender, 
           e.level AS level
           FROM staging_events AS e 
           WHERE e.page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs  ( song_id , 
                          title  , 
                      artist_id  , 
                           year  , 
                       duration )
    SELECT DISTINCT s.song_id AS song_id,
            s.title AS title,
           s.artist_id AS artist_id, 
           s.year AS year, 
           s.duration AS duration
           FROM staging_songs AS s;
""")

artist_table_insert = ("""
    INSERT INTO artists ( artist_id , 
                               name , 
                           location , 
                          lattitude , 
                          longitude  )
    SELECT DISTINCT s.artist_id AS artist_id,
                  s.artist_name AS name,
              s.artist_location AS location,
             s.artist_latitude AS lattitude, 
             s.artist_longitude AS longitude
             FROM staging_songs AS s;
""")

time_table_insert = ("""
    INSERT INTO time (start_time ,
                            hour ,
                             day ,
                            week ,
                           month ,
                            year ,
                         weekday)
    SELECT  DISTINCT TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second'  AS start_time,
            EXTRACT(hour FROM start_time)    AS hour,
            EXTRACT(day FROM start_time)     AS day,
            EXTRACT(week FROM start_time)    AS week,
            EXTRACT(month FROM start_time)   AS month,
            EXTRACT(year FROM start_time)    AS year,
            EXTRACT(week FROM start_time)    AS weekday
    FROM staging_events AS e;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
