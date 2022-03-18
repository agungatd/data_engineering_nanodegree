import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    event_id bigint IDENTITY(0,1) PRIMARY KEY,
    artist varchar,
    auth varchar,
    firstName varchar,
    gender varchar,
    itemInSession int,
    lastName varchar,
    length float,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration float,
    sessionId int SORTKEY DISTKEY,
    song varchar,
    status int,
    ts bigint,
    userAgent varchar,
    userId int 
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    artist_id varchar SORTKEY DISTKEY,
    artist_latitude float,
    artist_longitude float,
    artist_location varchar,
    artist_name varchar,
    duration float,
    num_songs int,
    song_id varchar,
    title varchar,
    year int
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id int IDENTITY(0,1) SORTKEY,
    start_time timestamp NOT NULL,
    user_id int NOT NULL DISTKEY,
    level varchar,
    song_id varchar,
    artist_id varchar,
    session_id int,
    location varchar,
    user_agent varchar
);   
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id int SORTKEY,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar SORTKEY,
    title varchar,
    artist_id int,
    year int,
    duration float
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar SORTKEY,
    name varchar,
    location varchar,
    latitude float,
    longitude float
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp SORTKEY,
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
COPY staging_events FROM {}
CREDENTIALS 'aws_iam_role={}'
FORMAT AS JSON {}
REGION 'us-west-2';
""").format(
    config['S3']['LOG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['S3']['LOG_JSONPATH']
)

staging_songs_copy = ("""
COPY staging_songs FROM {}
CREDENTIALS 'aws_iam_role={}'
FORMAT AS JSON 'auto'
REGION 'us-west-2';
""").format(
    config['S3']['SONG_DATA'],
    config['IAM_ROLE']['ARN']
)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
)
SELECT DISTINCT
    TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
    se.userId AS user_id,
    se.level AS level,
    ss.song_id AS song_id,
    ss.artist_id AS artist_id,
    se.sessionId AS session_id,
    se.location AS location,
    se.userAgent AS user_agent
FROM staging_events se
JOIN staging_songs ss
    ON (se.artist = ss.artist_name)
    AND (se.song = ss.title)
    AND (se.length = ss.duration)
WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (
    user_id,
    first_name,
    last_name,
    gender,
    level
)
SELECT DISTINCT
    userId AS user_id,
    firstName AS first_name,
    lastName AS last_name,
    gender, 
    level
FROM staging_events
WHERE page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) 
SELECT DISTINCT 
    song_id, 
    title,
    artist_id,
    year,
    duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT 
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT 
    TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time, 
    EXTRACT(hr from start_time) AS hour,
    EXTRACT(d from start_time) AS day,
    EXTRACT(w from start_time) AS week,
    EXTRACT(mon from start_time) AS month,
    EXTRACT(yr from start_time) AS year, 
    EXTRACT(weekday from start_time) AS weekday 
    FROM staging_events se  
    WHERE se.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
