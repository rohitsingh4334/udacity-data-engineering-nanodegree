## Proposed schema design (Star Schema):

#### Dimension Tables:
1. **users** - users in the app
   - user_id, first_name, last_name, gender, level
2. **songs** - songs in music database
   - song_id, title, artist_id, year, duration
3. **artists** - artists in music database
   - artist_id, name, location, latitude, longitude
4. **time** - timestamps of records in songplays broken down into specific units
   - start_time, hour, day, week, month, year, weekday

#### Fact Table:
1. **songplays** - records in log data associated with song plays i.e. records with page NextSong
   - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#### DDL Statement:
```
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
```

```
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
```

```
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
```

```
create table users (
    user_id          integer          not null          sortkey          primary key,
    first_name       varchar          not null,
    last_name        varchar          not null,
    gender           varchar          not null,
    level            varchar          not null
)
```

```
create table songs (
    song_id          varchar          not null          sortkey          primary key,
    title            text             not null,
    artist_id        varchar          not null,
    year             integer          not null,
    duration         float
)
```
```
create table artists (
    artist_id          varchar          not null          sortkey          primary key,
    name               varchar          not null, 
    location           text, 
    latitude          float,
    longitude          float
)
```
```
create table time (
    start_time          TIMESTAMP          NOT NULL          DISTKEY          SORTKEY          PRIMARY KEY,
    hour                integer            not null, 
    day                 integer            not null,
    week                integer            not null,
    month               integer            not null,
    year                integer            not null,
    weekday             varchar            not null
)
```

### adhoc_analysis:

```
query: SELECT COUNT(*) FROM staging_events
    8056

query: SELECT COUNT(*) FROM staging_songs
    14896

query: SELECT COUNT(*) FROM songplays
    333

query: SELECT COUNT(*) FROM users
    104

query: SELECT COUNT(*) FROM songs
    14896

query: SELECT COUNT(*) FROM artists
    10025

query: SELECT COUNT(*) FROM time
    333
```

### Songlay Analysis:

Q1: Top most artist name:

select a.name as artist_name, count(*) from songplays s join artists a on s.artist_id=a.artist_id  group by a.name order by count(*) desc limit 1;

```
 artist_name          | count 
---------------------+-------
 Dwight Yoakam       |  37
```

Q2: Sex ratio on app:

select u.gender,count(s.*) from songplays s join users u on s.user_id=u.user_id group by u.gender; 

```
 gender | count 
--------+-------
 M      | 344
 F      | 133
```

Q3: How many paid users in app:

select count(*) from songplays where level='paid'  group by level; 
```
paid_users_cnt 
-------
  271
```

Q4: location of app users:

select location,count(*) from songplays where level='paid'  group by location; 
```
                location                 | count 
-----------------------------------------+-------
Winston-Salem, NC                        | 8
Marinette, WI-MI                         |10
Tampa-St. Petersburg-Clearwater, FL      |18
Atlanta-Sandy Springs-Roswell, GA        |16
Janesville-Beloit, WI                    |12
San Jose-Sunnyvale-Santa Clara, CA       |8
San Antonio-New Braunfels, TX            |2
Detroit-Warren-Dearborn, MI              |2
Chicago-Naperville-Elgin, IL-IN-WI       |15
Waterloo-Cedar Falls, IA                 |21
```
