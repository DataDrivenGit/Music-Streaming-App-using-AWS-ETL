import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
    Create a Apache Spark session to process the data.
    Keyword arguments:
    * N/A
    Output:
    * spark -- An Apache Spark session.
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Load JSON input data (song_data) from input_data path,
        process the data to extract song_table and artists_table, and
        store the queried data to parquet files.
    Keyword arguments:
    * spark         -- reference to Spark session.
    * input_data    -- path to input_data to be processed (song_data)
    * output_data   -- path to location to store the output (parquet files).
    Output:
    * songs_table   -- directory with parquet files
                       stored in output_data path.
    * artists_table -- directory with parquet files
                       stored in output_data path.
    """
    
    # get filepath to song data file
    song_data = input_data + "song_data/A/A/A/TRAAAOF128F429C156.json"
    
    # read song data file
    print("=====Reading Song data=====")
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("song_data_table")
    
    
    # extract columns to create songs table
    print("=====Extracting Song table columns=====")
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').drop_duplicates()
       
    # write songs table to parquet files partitioned by year and artist
    print("=====Writing Song data=====")
    songs_table_path = output_data + "songs_table"
    songs_table.write.partitionBy("year", "artist_id").mode('overwrite').parquet(songs_table_path)


    # extract columns to create artists table
    print("=====Extracting artists table columns=====")
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude').drop_duplicates()
    
    # write artists table to parquet files
    print("=====Writing artists data=====")
    artists_table_path = output_data + "artists_table"     
    artists_table.write.mode('overwrite').parquet(artists_table_path)
    return songs_table, artists_table


def process_log_data(spark, input_data, output_data):
    """
    Load JSON input data (log_data) from input_data path,
        process the data to extract users_table, time_table,
        songplays_table, and store the queried data to parquet files.
    Keyword arguments:
    * spark            -- reference to Spark session.
    * input_data       -- path to input_data to be processed (log_data)
    * output_data      -- path to location to store the output
                          (parquet files).
    Output:
    * users_table      -- directory with users_table parquet files
                          stored in output_data path.
    * time_table       -- directory with time_table parquet files
                          stored in output_data path.
    * songplayes_table -- directory with songplays_table parquet files
                          stored in output_data path.
    """

    # get filepath to log data file
    log_data = input_data + 'log_data/2018/11/2018-11-15-events.json'

    
    # read log data file
    print("=====Reading log data=====")
    df = spark.read.json(log_data)
    

    # filter by actions for song plays
    print("=====Filtering log data=====")
    df = df.filter(df.page == 'NextSong').filter(df.userId.isNotNull()).filter(df.ts.isNotNull())
    df.createOrReplaceTempView("log_data_table")

    
    # extract columns for users table
    print("=====Extracting users table columns=====")
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').drop_duplicates()
    
    # write users table to parquet files
    print("=====Writing users data=====")
    users_table_path = output_data + "users_table"
    users_table.write.mode('overwrite').parquet(users_table_path)
    
    
    # extract columns for time table
    print("=====Extracting time table columns=====")
    time_table = spark.sql("""
                                SELECT 
                                    TT.time as start_time,
                                    hour(TT.time) as hour,
                                    dayofmonth(TT.time) as day,
                                    weekofyear(TT.time) as week,
                                    month(TT.time) as month,
                                    year(TT.time) as year,
                                    dayofweek(TT.time) as weekday
                                FROM (
                                    SELECT 
                                        to_timestamp(ldt.ts/1000) as time
                                    FROM log_data_table ldt
                                    WHERE ldt.ts IS NOT NULL
                                ) TT
                            """)
    
    # write time table to parquet files partitioned by year and month
    print("=====Writing time data=====")
    time_table_path = output_data + "time_table"
    time_table.write.partitionBy('year', 'month').mode('overwrite').parquet(time_table_path)



    # extract columns from joined song and log datasets to create songplays table
    print("=====Extracting songplays table columns=====")
    songplays_table = spark.sql("""
                                    SELECT 
                                        monotonically_increasing_id() AS songplay_id,
                                        to_timestamp(logT.ts/1000) AS start_time,
                                        month(to_timestamp(logT.ts/1000)) AS month,
                                        year(to_timestamp(logT.ts/1000)) AS year,
                                        logT.userId AS user_id,
                                        logT.level AS level,
                                        songT.song_id AS song_id,
                                        songT.artist_id AS artist_id,
                                        logT.sessionId AS session_id,
                                        logT.location AS location,
                                        logT.userAgent AS user_agent
                                    FROM log_data_table logT
                                    JOIN song_data_table songT 
                                        ON logT.artist = songT.artist_name 
                                        AND logT.song = songT.title
                                """)
        
    
    
    # write songplays table to parquet files partitioned by year and month
    print("=====Writing songplays data=====")
    songplays_table_path = output_data + "songplays_table"
    songplays_table.write.partitionBy('year', 'month').mode('overwrite').parquet(songplays_table_path)
    
    return users_table, time_table, songplays_table



def table_schema(spark,table_name):
    """
    Provides schema of table provided.
    Keyword arguments:
    * spark            -- spark session
    * table_name       -- name of table whose schema we want to check
    Output:
    * schema           -- schema of the created dataframe
    """
    
    table_name.printSchema()
    return

def count_rows(spark, table):
    """
    provide number of rows in a final tables.
    Keyword arguments:
    * spark            -- spark session
    * table            -- name of table whose rows we want to count
    Output:
    * count            -- count of rows in given table
    """
    return table.count()


def sample_data( spark, table_name):

    """
    provide sample data for a given table.
    Keyword arguments:
    * spark            -- spark session
    * table_name       -- name of table whose sample data we want to check
    Output:
    * rows             -- number of rows on a given dataframe
    """
    
    table_name.show(5, truncate=False)
    return
    



    
    
def main():
    print("=====Creating Spark Session=====\n")
    spark = create_spark_session()
    
    input_data = "s3a://udacity-dend/"
    output_data = config['AWS']['OUTPUT_DATA']
    
    print("=====Processing Song data=====\n")
    songs_table, artists_table = process_song_data(spark, input_data, output_data)
    
    print("=====Processing Log data=====\n")
    users_table, time_table, songplays_table = process_log_data(spark, input_data, output_data)
    print("=====data Processing Complete=====\n")
    
    print("=====check Processed tables and data=====\n")
    tables = ["songs_table", "artists_table", "users_table", "time_table", "songplays_table"]
    FinalTables = [songs_table, artists_table, users_table, time_table, songplays_table]

    for index,table in enumerate(tables):
        print(table+ " information\n")        
        print(table+ " Schema")
        table_schema(spark,FinalTables[index])
        
        print(table+ " number of rows: "+ str(count_rows(spark, FinalTables[index])))
        
        print(table+" Sample data")
        sample_data(spark,FinalTables[index])


if __name__ == "__main__":
    main()