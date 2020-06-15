# Data Modeling with Postgres

## The Purpose of Creating the sparkifydb Database

Sparkify, a startup company with a music streaming app, would like to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. They want to create a Postgres database with tables designed to optimize queries on song play analysis.

## Schema for Song Play Analysis

### **Fact Table**
**1. songplays** - records in log data associated with song plays i.e. records with page NextSong
 - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### **Dimension Tables**
**2. users** - users in the app
 - *user_id, first_name, last_name, gender, level*

**3. songs** - songs in music database
 - *song_id, title, artist_id, year, duration*

**4. artists** - artists in music database
 - *artist_id, name, location, lattitude, longitude*

**5. time** - timestamps of records in songplays broken down into specific units
 - *start_time, hour, day, week, month, year, weekday*

## ETL Pipeline

1. **Data** contains song_data and log_data. 

2. **test.ipynb** displays the first few rows of each table to check the database.

3. **create_tables.py** drops and creates the tables, which needs to be run before the ETL scripts to reset the tables.

4. **etl.ipynb** reads and processes a single file from song_data and log_data and loads the data into the tables.

5. **etl.py** reads and processes files from song_data and log_data and loads them into the tables.

6. **sql_queries.py** contains all the sql queries, and is imported into the last three files above.

7. **README.md** provides documentation on the project.

## Execute the below files in order each time before pipeline.
1. create_tables.py $ python create_tables.py
2. etl.ipynb/et.py $ python etl.py
3. test.ipynb => run in jupyter notebook