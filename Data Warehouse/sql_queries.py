import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
    artist varchar,
    auth varchar,
    firstName varchar,
    gender varchar,
    itemInSession varchar,
    lastName varchar,
    length float,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration varchar,
    sessionId int,
    song varchar,
    status int,
    ts varchar,
    userAgent varchar,
    userId int
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    num_songs int,
    artist_id varchar,
    artist_latitude varchar,
    artist_longitude varchar,
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration float,
    year int
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id int PRIMARY KEY NOT NULL IDENTITY(0,1), 
    start_time timestamp NOT NULL, 
    user_id varchar NOT NULL, 
    level varchar, 
    song_id varchar, 
    artist_id varchar, 
    session_id int, 
    location varchar, 
    user_agent varchar
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id varchar PRIMARY KEY NOT NULL, 
    first_name varchar, 
    last_name varchar, 
    gender varchar, 
    level varchar UNIQUE
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar PRIMARY KEY NOT NULL, 
    title varchar, 
    artist_id varchar NOT NULL, 
    year int, 
    duration float)\
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar PRIMARY KEY NOT NULL, 
    name varchar, 
    location varchar, 
    latitude varchar, 
    longitude varchar
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp PRIMARY KEY NOT NULL, 
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday int
)
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from 's3://udacity-dend/log_data' 
iam_role 'arn:aws:iam::648204718466:role/myRedshiftRole'
region 'us-west-2' compupdate off
JSON 's3://udacity-dend/log_json_path.json';
""")
#credentials 'aws_iam_role={arn:aws:iam::648204718466:role/myRedshiftRole}'
staging_songs_copy = ("""
copy staging_songs from 's3://udacity-dend/song_data' 
iam_role 'arn:aws:iam::648204718466:role/myRedshiftRole'
region 'us-west-2' compupdate off
JSON 'auto' truncatecolumns;
""")

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
    SELECT DISTINCT 
        TIMESTAMP 'epoch' + se.ts/1000 *INTERVAL '1 second' as start_time, 
        se.userId, 
        se.level,
        ss.song_id,
        ss.artist_id,
        se.sessionId,
        se.location,
        se.userAgent
    FROM staging_events se, staging_songs ss
    WHERE se.page = 'NextSong'
    AND se.song = ss.title
    AND se.userId NOT IN (
    SELECT DISTINCT s.user_id FROM songplays s WHERE s.user_id = user_id
        AND s.start_time = start_time AND s.session_id = session_id )
""")

user_table_insert = ("""INSERT INTO users (
    user_id, 
    first_name, 
    last_name, 
    gender, 
    level)
    SELECT DISTINCT 
        userId,
        firstName,
        lastName,
        gender, 
        level
    FROM staging_events
    WHERE page = 'NextSong'
    AND userId NOT IN (SELECT DISTINCT userId FROM users)
""")
#    ON CONFLICT ON CONSTRAINT users_level_key
#    DO UPDATE
#        SET level = EXCLUDED.level

song_table_insert = ("""INSERT INTO songs (
    song_id, 
    title, 
    artist_id, 
    year, 
    duration)
    SELECT DISTINCT 
        song_id, 
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE song_id NOT IN (SELECT DISTINCT song_id FROM songs)
""")

artist_table_insert = ("""INSERT INTO artists (
    artist_id, 
    name, 
    location, 
    latitude, 
    longitude)
    SELECT DISTINCT 
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
    WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists)
""")


time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT 
        start_time, 
        EXTRACT(hour from start_time) AS hour,
        EXTRACT(day from start_time) AS day,
        EXTRACT(week from start_time) AS week,
        EXTRACT(month from start_time) AS month,
        EXTRACT(year from start_time) AS year, 
        EXTRACT(weekday from start_time) AS weekday 
    FROM (
    	SELECT DISTINCT  TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time 
        FROM staging_events ss     
    )
    WHERE start_time NOT IN (SELECT DISTINCT start_time FROM time)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
