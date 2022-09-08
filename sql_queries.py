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
CREATE TABLE IF NOT EXISTS staging_events (
artist varchar(max),
auth varchar(max),
first_name varchar(max),
gender varchar(max),
item_in_session int,
last_name varchar(max),
length float,
level varchar(max),
location varchar(max),
method varchar(max),
page varchar(max),
registration varchar,
session_id int,
song varchar(max),
status int,
ts bigint,
user_agent varchar(max),
user_id int
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
num_songs int,
artist_id varchar(max),
artist_latitude float,
artist_longitude float,
artist_location varchar(max),
artist_name varchar,
song_id varchar(max),
title varchar(max),
duration float,
year int
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
songplay_id bigint identity(0,1) PRIMARY KEY, 
start_time timestamp NOT NULL, 
user_id int NOT NULL,
level varchar(max),
song_id varchar(max), 
artist_id varchar(max),
session_id int, 
location varchar(max), 
user_agent varchar(max)
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
user_id int PRIMARY KEY, 
first_name varchar(max), 
last_name varchar(max), 
gender varchar(max),
level varchar(max)
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
song_id varchar PRIMARY KEY, 
title varchar NOT NULL, 
artist_id varchar(max), 
year int,
duration float NOT NULL
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
artist_id varchar PRIMARY KEY, 
name varchar NOT NULL, 
location varchar(max), 
latitude float,
longitude float
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
start_time timestamp PRIMARY KEY,
hour int,
day int,
week int,
month int,
year int,
weekday int
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
credentials 'aws_iam_role={}'
compupdate on region 'us-west-2'
format as json {}
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
copy staging_songs from {}
credentials 'aws_iam_role={}'
compupdate on region 'us-west-2'
format as json 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT timestamp 'epoch' + se.ts/1000 * interval '1 second' as start_time, 
se.user_id, se.level, ss.song_id, ss.artist_id, se.session_id, se.location, se.user_agent
FROM staging_events se JOIN staging_songs ss ON (se.length = ss.duration AND se.song = ss.title AND se.artist = ss.artist_name)
WHERE se.page = 'NextSong'
AND se.user_id IS NOT NULL;
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT(user_id) user_id, first_name, last_name, gender, level
FROM staging_events
WHERE user_id IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT(song_id) song_id, title, artist_id, year, duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT(artist_id) artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT(start_time) start_time,
EXTRACT(hour from start_time) AS hour,
EXTRACT(day from start_time) AS day,
EXTRACT(week from start_time) AS week,
EXTRACT(month from start_time) AS month,
EXTRACT(year from start_time) AS year,
EXTRACT(weekday from start_time) AS weekday
FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
