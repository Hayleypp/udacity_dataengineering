import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.selectExpr("song_id", "artist_id", "title", "year", "duration")
    songs_table = songs_table.dropDuplicates(['song_id'])
                               
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data+'songs/'+'songs.parquet', partitionBy=['year','artist_id'])

    # extract columns to create artists table
    artists_table = df['artist_id', 'artist_location', 'artist_latitude', 'artist_longitude', 'aritst_name']
    artists_table = artists_table.dropDuplicates(['artist_id'])

    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists.parquet'), 'overwrite')     
                               
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists.parquet'), 'overwrite')
    
    
def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/*.json")

    # read log data file
    df = spark.read.format("json").load(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page == "NextSong")
    
    # extract columns for users table
    df.createOrReplaceTempView("users_table_df")
    users_table = spark.sql("""SELECT DISTINCT userId, firstName, lastName, gender, level FROM users_table_df \n
                                WRHERE userId IS NOT NULL""")
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users'))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x)/1000)))
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select(col('datetime').alias('start_time'),
                          hour('datetime').alias('hour'),
                          dayofmonth('datetime').alias('day'),
                          weekofyear('datetime').alias('week'),
                          month('datetime').alias('month'),
                          year('datetime').alias('year'),
                          )
    time_table = time_table.dropDuplicates(['start_time'])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partionBy('year', 'month').parquet(output_data + '/timetable')

    # read in song data to use for songplays table
    song_df = spark.read.parquet("s3://udacity-dend/song_data")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, song_df.title == df.song)
    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id())
    songplays_table = songplays_table[['songplay_id', 'start_time', 'userId', 'level', 'song_id', 
                                      'artist_id', 'sessionId', 'location', 'userAgent','month','year']]

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partionBy("year", "month").parquet(output_data + 'songplays_table')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://datalakeproject"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
