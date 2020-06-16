import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop  = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop       = "DROP TABLE IF EXISTS songplays"
user_table_drop           = "DROP TABLE IF EXISTS users"
song_table_drop           = "DROP TABLE IF EXISTS songs"
artist_table_drop         = "DROP TABLE IF EXISTS artists"
time_table_drop           = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
                            CREATE TABLE IF NOT EXISTS staging_events (
                            artist VARCHAR,
                            auth VARCHAR, 
                            firstName VARCHAR, 
                            gender VARCHAR, 
                            itemInSession INT, 
                            lastName VARCHAR, 
                            length NUMERIC,
                            level VARCHAR, 
                            location VARCHAR,
                            method VARCHAR,
                            page VARCHAR, 
                            registration NUMERIC, 
                            sessionId INT, 
                            song VARCHAR, 
                            status INT, 
                            ts BIGINT,
                            userAgent VARCHAR, 
                            userId INT)
                            """)

staging_songs_table_create = ("""
                            CREATE TABLE IF NOT EXISTS staging_songs (
                            artist_id VARCHAR, 
                            artist_latitude NUMERIC, 
                            artist_location VARCHAR,
                            artist_longitude NUMERIC, 
                            artist_name VARCHAR, 
                            duration NUMERIC, 
                            num_songs INT, 
                            song_id VARCHAR, 
                            title VARCHAR, 
                            year INT)
                            """)

songplay_table_create = ("""
                        CREATE TABLE IF NOT EXISTS songplays (
                        songplay_id INT IDENTITY(1,1) PRIMARY KEY, 
                        start_time TIMESTAMP NOT NULL, 
                        user_id INT NOT NULL, 
                        level VARCHAR, 
                        song_id VARCHAR, 
                        artist_id VARCHAR , 
                        session_id INT, 
                        location VARCHAR, 
                        user_agent VARCHAR)
                        """)

user_table_create = ("""
                    CREATE TABLE IF NOT EXISTS users (
                    user_id INT PRIMARY KEY, 
                    first_name VARCHAR NOT NULL, 
                    last_name VARCHAR NOT NULL, 
                    gender VARCHAR, 
                    level VARCHAR)
                    """)

song_table_create = ("""
                    CREATE TABLE IF NOT EXISTS songs (
                    song_id VARCHAR PRIMARY KEY, 
                    title VARCHAR, 
                    artist_id VARCHAR, 
                    year INT, 
                    duration NUMERIC)
                    """)

artist_table_create = ("""
                    CREATE TABLE IF NOT EXISTS artists (
                    artist_id VARCHAR PRIMARY KEY, 
                    name VARCHAR, 
                    location VARCHAR, 
                    lattitude NUMERIC, 
                    longitude NUMERIC)
                    """)

time_table_create = ("""
                    CREATE TABLE IF NOT EXISTS time (
                    start_time TIMESTAMP PRIMARY KEY, 
                    hour INT, 
                    day INT, 
                    week INT, 
                    month INT, 
                    year INT, 
                    weekday VARCHAR)
                    """)

# STAGING TABLES

staging_events_copy = ("""
                    copy staging_events from {}
                    iam_role {}
                    json{};
                    """).format(config['S3']['LOG_DATA'] ,\
                    config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
                    copy staging_songs from {}
                    iam_role {}
                    json'auto';
                    """).format(config['S3']['SONG_DATA'] ,\
                                config['IAM_ROLE']['ARN'])


# FINAL TABLES

songplay_table_insert = ("""
                        INSERT INTO songplays(
                        start_time,
                        user_id,
                        level,
                        song_id,
                        artist_id,
                        session_id,
                        location,
                        user_agent) 

                        SELECT DISTINCT TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second' AS start_time, 
                        e.userId AS user_id,  
                        e.level,  
                        s.song_id,  
                        s.artist_id,  
                        e.sessionId AS session_id,  
                        s.artist_location AS location,  
                        e.userAgent AS user_agent 

                        FROM staging_songs s 
                        RIGHT JOIN staging_events e 
                        ON s.title = e.song  AND s.artist_name = e.artist AND s.duration = e.length 
                        WHERE page = 'NextSong'
                        """)

user_table_insert = ("""
                    INSERT INTO users (
                    user_id,
                    first_name,
                    last_name,
                    gender,
                    level) 

                    SELECT 
                    DISTINCT userId AS user_id,  
                    firstName AS first_name,  
                    lastName AS last_name,  
                    gender,  
                    level 

                    FROM staging_events
                    WHERE page = 'NextSong'
                    """)
    
song_table_insert = ("""
                    INSERT INTO songs (
                    song_id,
                    title,
                    artist_id,
                    year,
                    duration) 

                    SELECT DISTINCT song_id,  
                    title,  
                    artist_id,  
                    year,  
                    duration 

                    FROM staging_songs
                    WHERE song_id IS NOT NULL
                    """)
    
artist_table_insert = ("""
                    INSERT INTO artists (
                    artist_id, 
                    name, 
                    location,
                    lattitude,
                    longitude) 

                    SELECT DISTINCT artist_id,  
                    artist_name AS name,  
                    artist_location AS location,  
                    artist_latitude AS lattitude,  
                    artist_longitude AS longitude 

                    FROM staging_songs
                    WHERE artist_id IS NOT NULL
                    """)

time_table_insert = ("""
                    INSERT INTO time (
                    start_time,
                    hour,
                    day,
                    week,
                    month,
                    year,
                    weekday) 

                    SELECT DISTINCT TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second' as start_time, 
                    EXTRACT (hour from TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second') AS hour, 
                    EXTRACT (day from TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second')AS day, 
                    EXTRACT (week from TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second') AS week, 
                    EXTRACT (month from TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second') AS month, 
                    EXTRACT (year from TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second') AS year, 
                    EXTRACT (dayofweek from TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second') AS weekday 

                    FROM staging_events as e
                    """)

# TEST
songplays_table_test = "SELECT * FROM songplays LIMIT 5"
users_table_test     = "SELECT * FROM users LIMIT 5"
songs_table_test     = "SELECT * FROM songs LIMIT 5"
artists_table_test   = "SELECT * FROM artists LIMIT 5"
time_table_test      = "SELECT * FROM time LIMIT 5"


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [ user_table_insert, song_table_insert,songplay_table_insert, artist_table_insert, time_table_insert]
test_queries = [songplays_table_test, users_table_test, songs_table_test, artists_table_test, time_table_test]
