import configparser
from datetime import datetime
import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import date_format
from pyspark.sql.types import TimestampType, DateType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['DEFAULT']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['DEFAULT']['AWS_SECRET_ACCESS_KEY']


def create_spark_session() -> SparkSession:
    '''
    Creates a new Spark session based on the options set in this builder.
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark: SparkSession,
                      input_data: str, output_data: str) -> None:
    '''
    Description:
    ETL process for song data. Function creates: songs and artists 
    tables.
    
    Parametres:
    spark - instance of a Spark session.
    input_data - path to log_data location.
    output_data - path to where log_data will be stored.
    '''
    # getting filepath to song data file
    song_data = 'song_data/*/*/*'
    
    # reading song data file
    df = spark.read.json(input_data + song_data)

    # creating and saving songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year',     'duration').filter(df['title'].isNotNull())
    
    songs_table = songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(output_data + "songs")

    # creating and saving artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude') \
                    .filter(df['artist_name'].isNotNull())
    
    artists_table.write.mode('overwrite').parquet(output_data + 'artists')


def process_log_data(spark: SparkSession,
                      input_data: str, output_data: str) -> None:
    '''
    Description:
    ETL process for log_data. Function creates: users, time and songplays 
    tables.
    
    Parametres:
    spark - instance of a Spark session.
    input_data - path to log_data location.
    output_data - path to where log_data will be stored.
    '''   
    # setting filepath to log data file
    log_data = 'log_data/*/*/'

#     # reading  log data file
    df = spark.read.json(input_data + log_data)
    
#     # filtering df by actions for song plays
    df = df.filter( df['page']=='NextSong')

#     # creating and saving users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level') \
    .filter(df['userId'].isNotNull())
    
    users_table.write.mode('overwrite').parquet(output_data + 'users')

    # creating timestamp and datetime columns for time_table creation
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000), TimestampType())
    df = df.withColumn('ts_timestamp', get_timestamp(col('ts')))
    
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000), DateType())
    df =  df.withColumn('ts_datetime', get_datetime(col('ts')))
    
    # creating and saving time_table
    time_table = df.select(
        df['ts_datetime'].alias('start_time'),
        date_format(col('ts_datetime'),'H').alias('hour'),
        date_format(col('ts_datetime'),'d').alias('day'),
        date_format(col('ts_datetime'),'w').alias('week'),
        date_format(col('ts_datetime'),'M').alias('month'),
        date_format(col('ts_datetime'),'y').alias('year'),
        date_format(col('ts_datetime'),'u').alias('weekday')
    ).filter(df['ts_datetime'].isNotNull())
    
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + "time")

    # importing song_df for sonplays table creation
    song_df = spark.read.json(input_data + 'song_data/*/*/*')
    
    # creating and saving song_plays table
    songplays_table = song_df.join(df, song_df.artist_name == df.artist, "inner").select(
     df['ts_datetime'].alias('start_time'), 'userId', 'level',
    'song_id', 'artist_id', 'sessionId', 'location',  'userAgent')
    
    songplays_table.write.mode('overwrite').parquet(output_data + "songplays")


def main() -> None:
    '''
    Creates main logic of an ETL script by combining and calling previously defined functions.
    '''
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "" # path to where you want to save the data; preferably AWS s3 bucket
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
