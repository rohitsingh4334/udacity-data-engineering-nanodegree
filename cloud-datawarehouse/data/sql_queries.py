import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events;"
staging_songs_table_drop = "drop table if exists staging_songs;"
songplay_table_drop = "drop table if exists songplays;"
user_table_drop = "drop table if exists users;"
song_table_drop = "drop table if exists songs;"
artist_table_drop = "drop table if exists artists;"
time_table_drop = "drop table if exists time;"

# CREATE TABLES

staging_events_table_create= ("""
create table staging_events (
    artist         varchar,
    auth           varchar,
    firstName      varchar,
    gender         varchar,
    itemInSession  integer,
    lastName       varchar,
    length         float,
    level          varchar,
    location       text,
    method         varchar,
    page           varchar,
    registration   float,
    sessionId      integer,
    song           text,
    status         integer,
    ts             timestamp,
    userAgent      text,
    userId         integer
)
""")

staging_songs_table_create = ("""
create table staging_songs (
    num_songs          integer,
    artist_id          varchar,
    artist_latitude    float,
    artist_longitude   float,
    artist_location    text,
    artist_name        varchar,
    song_id            varchar,
    title              varchar,
    duration           float,
    year               integer
)
""")

songplay_table_create = ("""
create table songplays (
    songplay_id           integer          IDENTITY(0,1)          primary key,
    start_time            timestamp        not null               sortkey          distkey,
    user_id               integer          not null,
    level                 varchar,
    song_id               varchar          not null,
    artist_id             varchar          not null,
    session_id            integer, 
    location              varchar, 
    user_agent            text
)
""")

user_table_create = ("""
create table users (
    user_id          integer          not null          sortkey          primary key,
    first_name       varchar          not null,
    last_name        varchar          not null,
    gender           varchar          not null,
    level            varchar          not null
)
""")

song_table_create = ("""
create table songs (
    song_id          varchar          not null          sortkey          primary key,
    title            text             not null,
    artist_id        varchar          not null,
    year             integer          not null,
    duration         float
)
""")

artist_table_create = ("""
create table artists (
    artist_id          varchar          not null          sortkey          primary key,
    name               varchar          not null, 
    location           text, 
    latitude          float,
    longitude          float
)
""")

time_table_create = ("""
create table time (
    start_time          TIMESTAMP          NOT NULL          DISTKEY          SORTKEY          PRIMARY KEY,
    hour                integer            not null, 
    day                 integer            not null,
    week                integer            not null,
    month               integer            not null,
    year                integer            not null,
    weekday             varchar            not null
)
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {data_bucket}
    access_key_id '{access_key}'
    secret_access_key '{secret}'
    region 'us-west-2' format as JSON {log_json_path}
    timeformat as 'epochmillisecs' 
""").format(
    data_bucket=config['S3']['LOG_DATA'], access_key=config['IAM_ROLE']['Access_key_ID'], 
    secret=config['IAM_ROLE']['Secret_access_key'], log_json_path=config['S3']['LOG_JSONPATH']
)

staging_songs_copy = ("""
    copy staging_songs from {data_bucket}
    access_key_id '{access_key}'
    secret_access_key '{secret}'
    region 'us-west-2' format as JSON 'auto'
""").format(
    data_bucket=config['S3']['SONG_DATA'],
    access_key=config['IAM_ROLE']['Access_key_ID'], 
    secret=config['IAM_ROLE']['Secret_access_key']
)

# FINAL TABLES

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT(userId) as user_id, firstName as first_name, lastName as last_name, gender, level
    FROM staging_events
    WHERE userId IS NOT NULL AND page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT  DISTINCT(song_id) AS song_id,title,artist_id,year,duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO  artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT(artist_id) as artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT  DISTINCT(start_time)                AS start_time,
            EXTRACT(hour FROM start_time)       AS hour,
            EXTRACT(day FROM start_time)        AS day,
            EXTRACT(week FROM start_time)       AS week,
            EXTRACT(month FROM start_time)      AS month,
            EXTRACT(year FROM start_time)       AS year,
            EXTRACT(dayofweek FROM start_time)  as weekday
    FROM songplays
""")

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT  DISTINCT(e.ts)  AS start_time, 
            e.userId        AS user_id, 
            e.level         AS level, 
            s.song_id       AS song_id, 
            s.artist_id     AS artist_id, 
            e.sessionId     AS session_id, 
            e.location      AS location, 
            e.userAgent     AS user_agent
    FROM staging_events e
    JOIN staging_songs  s   ON (e.song = s.title AND e.artist = s.artist_name)
    AND e.page  = 'NextSong'
""")

# GET NUMBER OF ROWS IN EACH TABLE
get_number_staging_events = ("""
    SELECT COUNT(*) FROM staging_events
""")

get_number_staging_songs = ("""
    SELECT COUNT(*) FROM staging_songs
""")

get_number_songplays = ("""
    SELECT COUNT(*) FROM songplays
""")

get_number_users = ("""
    SELECT COUNT(*) FROM users
""")

get_number_songs = ("""
    SELECT COUNT(*) FROM songs
""")

get_number_artists = ("""
    SELECT COUNT(*) FROM artists
""")

get_number_time = ("""
    SELECT COUNT(*) FROM time
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
select_number_rows_queries= [get_number_staging_events, get_number_staging_songs, get_number_songplays, get_number_users, get_number_songs, get_number_artists, get_number_time]