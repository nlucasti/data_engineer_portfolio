#import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import pyspark.sql.functions as F

#config = configparser.ConfigParser()
#config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']='AKIAJSFBIUXU3LVRGRKA' #config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']='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' #config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    This function will be responsible for creating the spark session

    Parameters:
        None 
    Returns:
        None

   """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
        #.config("spark.dynamicAllocation.enabled", false)
        #.config("spark.executor.cores", 9)
        #.config("spark.executors.memory", "10G")
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function will be responsible for processing the song data from S3 
    then partitioning it and reuploading to S3

    Parameters:
        spark - the current spark session
        input_data - path of input_data, in this case, S3
        output_data - path of output_data, in this case, S3
    Returns:
        None
   """
    
    # get filepath to song data file
    print('Processing song and artist data:')
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)
    print('Read song data.')
    
    # extract columns to create songs table
    print('Creating song table:')
    songs_table = df.select("song_id","title","artist_id","year","duration")
    print('Partitioning song data:')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year","artist_id").parquet(output_data+"/songs/songs.parquet")
    print('Partitions created.')
    
    # extract columns to create artists table
    print('Creating artist table:')
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")    
    print('Partitioning artist data:')
    
    # write artists table to parquet files
    print('Partitioning artist data:')
    artists_table.write.mode('overwrite').parquet(output_data+"/artists/artists.parquet")
    print('Partitions created.')

def process_log_data(spark, input_data, output_data):
     """
    This function will be responsible for processing the log data from S3 
    then partitioning it and reuploading to S3

    Parameters:
        spark - the current spark session
        input_data - path of input_data, in this case, S3
        output_data - path of output_data, in this case, S3
    Returns:
        None
   """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    print('Reading log data:')
    df = spark.read.json(log_data)
    print('Read log data.')
    
    # filter by actions for song plays
    df = df.filter("page=='NextSong'")

    # extract columns for users table  
    print('Creating users table:')
    users_table = df.selectExpr(["userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"])
    
    # write users table to parquet files
    print('Partitioning user data:')
    users_table.write.mode('overwrite').parquet(output_data+"/users/users.parquet")
    print('Partitions created.')
    
    # create timestamp column from original timestamp column
    df = df.withColumn("timestamp",  F.to_timestamp(df.ts/1000))
    
    # create datetime column from original timestamp column
    df = df.withColumn("datetime",  F.to_date(df.timestamp))
    
    # extract columns to create time table
    print('Creating time table:')
    time_table = df.selectExpr(["timestamp as start_time","hour(datetime) as hour", "dayofmonth(datetime) as day",
                                 "weekofyear(datetime) as week", "month(datetime) as month", "year(datetime) as year", 
                                 "dayofweek(datetime) as weekday"])
                                                  
    # write time table to parquet files partitioned by year and month
    print('Partitioning time data:')
    time_table.write.mode('overwrite').partitionBy("year","month").parquet(output_data+"/time/time.parquet")
    print('Partitions created')
    
    # read in song data to use for songplays table
    print('Reading song parquet files:')
    song_df = spark.read.parquet(output_data+"/songs/songs.parquet")
    
    print('Reading artist parquet files:')
    artist_df = spark.read.parquet(output_data+"/artists/artists.parquet")
    
    # extract columns from joined song and log datasets to create songplays table 
    print('Creating songplays table:')
    songplays_table = df.join(song_df,df.song==song_df.title)\
                        .join(artist_df, df.artist==artist_df.artist_name)\
                        .join(time_table, df.timestamp==time_table.start_time)\
                        .select(df.timestamp.alias("start_time"),
                                time_table.year,
                                time_table.month,
                                df.userId.alias("user_id"), 
                                df.level, 
                                song_df.song_id, 
                                artist_df.artist_id, 
                                df.sessionId.alias("session_id"), 
                                df.location, 
                                df.userAgent.alias("user_agent"))

    # write songplays table to parquet files partitioned by year and month
    print('Partitioning songplays data:')
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+"/songplays/songplays.parquet")
    print('Partitions created.')
    
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dend-nlucasti/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
