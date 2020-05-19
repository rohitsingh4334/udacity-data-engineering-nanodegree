# Sparkifydb
This is a project for Sparkifydb where I need to implement schema design in postgresql RDBMS.

#### Directory Structure for Project Data:
```
├── create_tables.py # drop tables if exists and create table for project Sparkifydb.
├── data # collection of log and song data in the form of .json files.
│   ├── log_data # usefule for users, time dimensional tables and songplays fact table.
│   └── song_data
├── etl.ipynb # interactive notebook etl process.
├── etl.py # script for etl process 
├── README.md # contains the project related information.
├── sql_queries.py # constains postgresql related create, insert and select statements. 
└── test.ipynb # interactive notebook for adhoc queries on sparkiyfy database.
```

### Purpose Solution:
- Start Schema Design, for Sparkify is suitable to use this type of schema design and the reason is that the dimensional tables maintain the static attributes related to the artist, song, user and time, but the fact tables could gives the insights like top users among gender, most pouplar artitst, most popular song etc.

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


#### DDL Statement for Sparkifydb:

```
create table if not exists users(
user_id int,
first_name varchar,
last_name varchar,
gender varchar,
level varchar,
Pirmary key(user_id)
);
```

```
create table if not exists artists(
artist_id varchar, 
name varchar, 
location text, 
latitude decimal(10,5), 
longitude decimal(10,5),
Pirmary key(artist_id)
);
```

```
create table if not exists time(
start_time timestamp, 
hour int, 
day int, 
week int, 
month int, 
year int, 
weekday int,
Pirmary key(start_time)
);
```

```
create table if not exists songplays(
songplay_id SERIAL,
start_time timestamp,
user_id int not null,
level varchar not null,
song_id varchar not null,
artist_id varchar not null,
session_id int not null,
location text,
user_agent text,
Pirmary key(songplay_id)
);
```

intial commands:

1. ``` python create_tables.py # to create table in sparkifydb```
2. ``` python etl.py # to add data into tables.```

### Songlay Analysis:

Q1: Top most artist name:

select a.name as artist_name, count(*) from songplays s join artists a on s.artist_id=a.artist_id  group by a.name order by count(*) desc limit 1;

```
 artist_name | count 
-------------+-------
 Elena       |     1
```

Q2: Sex ratio on app:

select u.gender,count(s.*) from songplays s join users u on s.user_id=u.user_id group by u.gender; 

```
 gender | count 
--------+-------
 M      | 25928
 F      | 71980
```

Q3: How many paid users in app:

select count(*) from songplays where level='paid'  group by level; 
```
paid_users_cnt 
-------
  5933
```

Q4: location of app users:

select location,count(*) from songplays where level='paid'  group by location; 
```
                location                 | count 
-----------------------------------------+-------
 San Jose-Sunnyvale-Santa Clara, CA      |   196
 Chicago-Naperville-Elgin, IL-IN-WI      |   462
 Sacramento--Roseville--Arden-Arcade, CA |   253
 Birmingham-Hoover, AL                   |   267
 Longview, TX                            |    17
 Janesville-Beloit, WI                   |   288
 Atlanta-Sandy Springs-Roswell, GA       |   428
 Waterloo-Cedar Falls, IA                |   433
 Lake Havasu City-Kingman, AZ            |   336
 San Antonio-New Braunfels, TX           |    33
 Winston-Salem, NC                       |   213
 Lansing-East Lansing, MI                |   557
 Tampa-St. Petersburg-Clearwater, FL     |   307
 Marinette, WI-MI                        |   169
 New York-Newark-Jersey City, NY-NJ-PA   |   149
 Augusta-Richmond County, GA-SC          |   140
 Portland-South Portland, ME             |   648
 Detroit-Warren-Dearborn, MI             |    72
 Red Bluff, CA                           |   205
 San Francisco-Oakland-Hayward, CA       |   760
```

