# DROP TABLES

songplay_table_drop = "drop table if exists songplays_fact;"
user_table_drop = "drop table if exists users_dim;"
song_table_drop = "drop table if exists songs_dim;"
artist_table_drop = "drop table if exists artists_dim;"
time_table_drop = "drop table if exists time_dim;"

# CREATE TABLES

songplay_table_create = ("""
create table if not exists songplays(
songplay_id SERIAL,
start_time timestamp,
user_id int,
level varchar,
song_id varchar,
artist_id varchar,
session_id int,
location text,
user_agent text
);
""")

user_table_create = ("""
create table if not exists users(
user_id int,
first_name varchar,
last_name varchar,
gender varchar,
level varchar
);
""")

song_table_create = ("""
create table if not exists songs(
song_id varchar, 
title text, 
artist_id varchar, 
year int, 
duration decimal(10,5)
)
""")

artist_table_create = ("""
create table if not exists artists(
artist_id varchar, 
name varchar, 
location text, 
latitude decimal(10,5), 
longitude decimal(10,5)
);
""")

time_table_create = ("""
create table if not exists time(
start_time timestamp, 
hour int, 
day int, 
week int, 
month int, 
year int, 
weekday int
);
""")

# INSERT RECORDS

songplay_table_insert = ("""
insert into songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
insert into users(user_id, first_name, last_name, gender, level)
VALUES (%s, %s, %s, %s, %s)
""")

song_table_insert = ("""
insert into songs(song_id, title, artist_id, year, duration)
VALUES (%s, %s, %s, %s, %s)
""")

artist_table_insert = ("""
insert into artists(artist_id, name, location, latitude, longitude)
VALUES (%s, %s, %s, %s, %s)
""")


time_table_insert = ("""
insert into time(start_time, hour, day, week, month, year, weekday)
VALUES (%s, %s, %s, %s, %s, %s, %s)
""")

# FIND SONGS

song_select = ("""
select s.song_id,a.artist_id  
from songs s join artists a on s.artist_id=a.artist_id 
where s.title=%s and a.name=%s and s.duration=%s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]