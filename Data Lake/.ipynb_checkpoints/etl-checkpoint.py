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
    # get filepath to song data file
    song_data = input_data + "song-data"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id","title","artist_id","year","duration").toPandas()
    st = songs_table.alias('st')  
    
    # write songs table to parquet files partitioned by year and artist
    st.write.partitionBy("year","artist").parquet(output_data+"/songs"")
    
    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")
    at = artists_table.alias('at')      
    
    # write artists table to parquet files
    at.write.parquet(output_data+"/artists")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log-data"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter("page=='NextSong'")

    # extract columns for users table    
    users_table = df.select("user_id", "first_name", "last_name", "gender", "level")
    ut = artists_table.alias('ut')
    
    # write users table to parquet files
    ut.write.parquet(output_data+"/users")

    # create timestamp column from original timestamp column
    df = df.withColumn("timestamp",  F.to_timestamp(df2.ts/1000))#get_time("ts"))
    
    # create datetime column from original timestamp column
    df = df.withColumn("datetime",  F.to_date(df2.timestamp))
    
    # extract columns to create time table
    time_table = df.selectExpr(["timestamp as start_time","hour(datetime)", "dayofmonth(datetime)",
                                 "weekofyear(datetime)", "month(datetime)", "year(datetime)", 
                                 "dayofweek(datetime)"])
    tt = time_table.alias('tt')
                                                  
    # write time table to parquet files partitioned by year and month
    tt.write.partitionBy("year","month").parquet(output_data+"/time")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data+"/songs")
    artist_df = spark.read.parquet(output_dat+"/artists")
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.select(df.timestamp.alias("start_time"), 
                                 df.userId.alias("user_id"), 
                                 "level", 
                                 "song", 
                                 "artist", 
                                 df.sessionId.alias("session_id"), 
                                 "location", 
                                df.userAgent.alias("user_agent")\
                                .join(song_df,df.song==song_df.title)\
                                .join(artists_df, df.artist==artists_df.artist_name)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year(datetime)", "month(datetime)").parquet(output_data+"/songplays")

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dend-nlucasti/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
