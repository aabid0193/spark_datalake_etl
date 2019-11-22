import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function reads the song data and populates song and artist tables.
        
    Inputs:
        spark: spark session object.
        input_data: path to the song data.
        output_data: path to where the parquet files will be saved.
        
    Returns:
        None
    """
    
    # get filepath to song data file
    song_data = f'{input_data}/song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id',
                            'year', 'duration').dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(f'{output_data}/songs_table',
                              mode='overwrite',
                              partitionBy=['year', 'artist_id'])

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name',
                              'artist_location', 'artist_latitude',
                              'artist_longitude').dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(f'{output_data}/artists_table',
                                mode='overwrite')


def process_log_data(spark, input_data, output_data):                     
    """
    This function reads the log data and populates songplays fact table and user, song and time tables.
        
    Inputs:
        spark: spark session object.
        input_data: path to the song data.
        output_data: path to where the parquet files will be saved.
        
    Returns:
        None
    """
                         
    # get filepath to log data file
    log_data = f'{input_data}/log_data/*/*/*.json'
    song_data = f'{input_data}/song_data/*/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df['page'] == 'NextSong')

    # extract columns for users table    
    user_table = df.select('userId', 'firstName', 'lastName',
                           'gender', 'level').dropDuplicates()
    
    # write users table to parquet files
    user_table.write.parquet(f'{output_data}/user_table', mode='overwrite')
    
    # extract columns to create time table
    df.createOrReplaceTempView("logs") 
    time_table = spark.sql('SELECT t.start_time, \
           hour(t.start_time) AS hour, \
           day(t.start_time) AS day, \
           weekofyear(t.start_time) AS week, \
           month(t.start_time) AS month, \
           year(t.start_time) AS year, \
           dayofweek(t.start_time) AS weekday\
           FROM \
           (SELECT from_unixtime(ts/1000) as start_time FROM logs GROUP BY start_time) t')
    
    # write time table to parquet files partitioned by year and month
    time_table.parquet(f'{output_data}/time_table', 
                       mode="overwrite" 
                       partitionBy=['year', 'month')

    # read in song data to use for songplays table
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table
    song_df.createOrReplaceTempView('songs')
    songplays_table = spark.sql('SELECT \
                      monotonically_increasing_id() as songplay_id,  \
                      from_unixtime(l.ts/1000) AS start_time, \
                      userId AS user_id,\
                      l.level,\
                      s.song_id,\
                      s.artist_id,\
                      l.sessionId AS session_id,\
                      l.location,\
                      l.userAgent AS user_agent\
                      FROM \
                      logs l \
                      LEFT JOIN songs s ON l.song = s.title') 
    songplays_table = songplays_table.withColumn('year',
                                                 year(songplays_table.start_time)).withColumn('month',
                                                                                              month(songplays_table.start_time))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(f'{output_data}/songplays_table',
                                  mode='overwrite',
                                  partitionBy=['year', 'month'])


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-udacity-ahmed"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
