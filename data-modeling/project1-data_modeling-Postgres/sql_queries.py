# DROP TABLES
songplay_table_drop = "DROP table IF EXISTS songplays"
user_table_drop     = "DROP table IF EXISTS users"
song_table_drop     = "DROP table IF EXISTS songs"
artist_table_drop   = "DROP table IF EXISTS artists"
time_table_drop     = "DROP table IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
                        CREATE TABLE IF NOT EXISTS songplays(
                        songplay_id SERIAL PRIMARY KEY,
                        start_time bigint NOT NULL,
                        user_id int NOT NULL, 
                        level varchar, 
                        song_id varchar, 
                        artist_id varchar,
                        session_id int,
                        location varchar,
                        user_agent varchar)
                        """)
user_table_create = ("""
                    CREATE TABLE IF NOT EXISTS users (
                    user_id int NOT NULL PRIMARY KEY,
                    first_name varchar,
                    last_name varchar,
                    gender varchar,
                    level varchar)
                    """)

# duration float?
song_table_create = ("""
                    CREATE TABLE IF NOT EXISTS songs (
                    song_id varchar NOT NULL PRIMARY KEY,
                    title varchar,
                    artist_id varchar,
                    year int,
                    duration float)
                    """)

# lattitude float, longitude float?
artist_table_create = ("""
                        CREATE TABLE IF NOT EXISTS artists (
                        artist_id varchar NOT NULL PRIMARY KEY,
                        name varchar,
                        location varchar,
                        lattitude float,
                        longitude float)
                        """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                    start_time time NOT NULL PRIMARY KEY,
                    hour int,
                    day int,
                    week int,
                    month int,
                    year int,
                    weekday varchar)
                    """)



# INSERT RECORDS

# Insert data using the COPY command to bulk insert log files instead of using INSERT on one row at a time
songplay_table_insert = ("""
                        INSERT INTO songplays 
                        (start_time, user_id, level, song_id, artist_id,session_id, location, user_agent) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s) 
                        ON CONFLICT (songplay_id) DO NOTHING                    
                        """)

user_table_insert = ("""
                    INSERT INTO users (user_id, first_name, last_name, gender, level) \
                    VALUES (%s, %s, %s, %s, %s) \
                    ON CONFLICT (user_id) DO UPDATE SET level=EXCLUDED.level
                    """)

song_table_insert = ("""
                     INSERT INTO songs (song_id, title, artist_id, year, duration)
                     VALUES (%s, %s, %s, %s, %s)
                     ON CONFLICT (song_id) DO NOTHING
                     """)

artist_table_insert = ("""
                    INSERT INTO artists (artist_id, name, location, lattitude, longitude)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (artist_id) DO NOTHING
                    """)


time_table_insert = ("""
                    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (start_time) DO NOTHING
                    """)

# FIND SONGS
#song ID and artist ID based on the title, artist name, and duration of a song.
song_select = ("""SELECT songs.song_id,
                artists.artist_id  
                FROM songs 
                JOIN artists ON songs.artist_id = artists.artist_id 
                WHERE songs.title = (%s) AND artists.name = (%s) AND songs.duration = (%s)
                """)

# ANALYSIS TABLES
songplays_table = "SELECT * FROM songplays LIMIT 5"
users_table     = "SELECT * FROM users LIMIT 5"
songs_table     = "SELECT * FROM songs LIMIT 5"
artists_table   = "SELECT * FROM artists LIMIT 5"
time_table      = "SELECT * FROM time LIMIT 5"

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries   = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
analysis_queries     = [songplays_table, users_table, songs_table, artists_table, time_table]