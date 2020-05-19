import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, DateType, TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create or Fetch a running Spark Session with spark jars for aws as a configuration.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function loads song_data from S3 and processes with the help of pyspark api.
    to extract songs and artist tables and then send back to S3.
    
    :Params:
    :spark: Spark Session object.
    :input_data: input S3 bucket path for song_data
    :output_data: outputS3 bucket path for extracting songs and artist tables.
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # song data schema
    song_data_schema = StructType([
        StructField("artist_id", StringType()),
        StructField("artist_location", StringType()),
        StructField("artist_latitude", DoubleType()),
        StructField("artist_longitude", DoubleType()),
        StructField("artist_name", StringType()),
        StructField("duration", TimestampType()),
        StructField("num_songs", IntegerType()),
        StructField("song_id", StringType()),
        StructField("title", StringType()),
        StructField("year", IntegerType())
    ])
    
    # read song data file
    df = spark.read.json(song_data, schema=song_data_schema)
    
    # creating or replacing a view for future use.
    df.createOrReplaceTempView('songs_data')
    
    # song columns as a list.
    song_columns = ['song_id', 'title', 'artist_id', 'year', 'duration'] 

    # extract columns to create songs table with no duplicates.
    songs_table = df.select(song_columns).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data + 'songs/')

    # column names list for artist table.
    artists_columns = [
        'artist_id', 'artist_name as name', 'artist_location as location', 'artist_latitude as latitude', 
        'artist_longitude as longitude'
    ]
    
    # extract columns to create artists table
    artists_table = df.selectExpr(artists_columns).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'artists/')


def process_log_data(spark, input_data, output_data):
    """
    This function loads log_data from S3 and processes with the help of pyspark api.
    Extracted songs and artist dimensional tables send back to S3.
    
    :Params:
    :spark : Spark Session object
    :input_data  : location of log_data json files with the events data
    :output_data : S3 bucket were dimensional tables in parquet format will be stored
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'
    
    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    
    # users table columns list
    users_columns = [
        "userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level",
    ]
    
    # extract columns for users table    
    users_table = df.selectExpr(users_columns).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'users/')

    # create or replace log_data view fpr further use..
    df.createOrReplaceTempView("log_data")
    
    # create timestamp column from original timestamp column
    # get_timestamp = udf(da)
    
    # create datetime column from original timestamp column
    #  get_datetime = udf()
    # df = 
    
    # extract columns to create time table with the help of to_timestamp inbuilt functions.
    time_table = df = spark.sql("""
        select 
            to_timestamp(ts/1000) as start_time,
            hour(to_timestamp(ts/1000)) as hour, 
            dayofmonth(to_timestamp(ts/1000)) day, 
            weekofyear(to_timestamp(ts/1000)) week, 
            month(to_timestamp(ts/1000)) month,
            year(to_timestamp(ts/1000)) year,
            dayofweek(to_timestamp(ts/1000)) as weekday
        from log_data
        where ts is not null
    """) 
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(output_data + 'time/')

    # read in song data to use for songplays table
   #  song_df = 

    # extract columns from joined song_data view and log_data view to create songplays table 
    songplays_table = spark.sql("""
    select 
        monotonically_increasing_id() as songplay_id,
        year(to_timestamp(ts/1000)) as year,
        month(to_timestamp(ts/1000)) as month,
        to_timestamp(ts/1000) as start_time,
        l.userId as user_id,
        l.level,
        s.song_id as song_id,
        s.artist_id as artist_id,
        l.sessionId as session_id,
        l.location,
        l.user_agent
    from log_data l
    JOIN song_data_table s on l.artist = s.artist_name and l.song = s.title
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'songplays/')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://rohit-sparkify-dend/"
    
    print("Processing Song Data......")
    process_song_data(spark, input_data, output_data) 
    print("Processing Log Data......")
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
