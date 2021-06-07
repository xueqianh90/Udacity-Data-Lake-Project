import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ["AWS_ACCESS_KEY_ID"]=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/A/A/A/*.json")
    
    # read song data file
    df = spark.read.json(song_data)

    df = df.withColumn("num_songs",df.num_songs.cast('int'))
    df = df.withColumn("year",df.year.cast('int'))

    # extract columns to create songs table

    songs_table = df.select('title', 'artist_id', 'year', 'duration').drop_duplicates()
    songs_table = songs_table.withColumn("song_id", monotonically_increasing_id())

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data + "songs")

    # extract columns to create artists table
    artists_table = df.selectExpr("artist_id", "artist_name as name", "artist_location as location", \
                                  "artist_latitude as latitude", "artist_longitude as longitude").drop_duplicates()

    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + 'artists')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/*.json")

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.selectExpr('userId as user_id', 'firstName as first_name', \
                                'lastName as last_name', 'gender', 'level').drop_duplicates()

    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + 'users')


    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x / 1000, TimestampType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), TimestampType())
    df = df.withColumn("start_time", get_datetime(df.timestamp))

    # extract columns to create time table

    df = df.withColumn("hour", hour("start_time")) \
            .withColumn("day", dayofmonth("start_time")) \
            .withColumn("week", weekofyear("start_time")) \
            .withColumn("month", month("start_time")) \
            .withColumn("year", year("start_time")) \
            .withColumn("weekday", dayofweek("start_time"))

    time_table = df.select("start_time", "hour", "day", "week", "month", "year", "weekday")

    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + 'time')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, "songs/*/*/*"))
    artist_df = spark.read.parquet(os.path.join(output_data, "artists"))

    # extract columns from joined song and log datasets to create songplays table 
    songplays = df.join(song_df, song_df.title == df.song)\
                .join(artist_df, artist_df.name == df.artist)\
                .join(time_table, time_table.start_time == df.start_time)\
                .select(df.start_time,
                        df.userId,
                        df.level,
                        song_df.song_id,
                        artist_df.artist_id,
                        df.sessionId,
                        df.location,
                        df.userAgent,
                        time_table.year,
                        time_table.month).drop_duplicates()

    songplays_table = songplays.withColumnRenamed("userId", "user_id")\
                               .withColumnRenamed("sessionId", "session_id")\
                               .withColumnRenamed("userAgent", "user_agent")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + 'songplays')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://xh-udacity-data-lake-project/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
