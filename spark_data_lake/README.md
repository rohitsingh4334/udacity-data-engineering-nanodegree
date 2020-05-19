# Project: Data Lake

## Introduction

*A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.*

In this project, we will build an spark based etl pipeline that extracts their data from S3 based data lake, processes them using Spark(Pysaprk) which will be deployed on an EMR cluster using AWS, and send back into S3 as a set of dimensional tables in the form of parquet format. 

## How to run

create a file `dl.cfg` (local mode)

```
KEY=YOUR_AWS_ACCESS_KEY
SECRET=YOUR_AWS_SECRET_KEY
```

Create an S3 Bucket named `rohit-udacity-sparkify-dend` where output results will be stored.

To run the process of etl: (local mode)
`python etl.py`

To run on EMR, you need to upload the whole script into Jupyter Notebook.

## Project structure

File Structure:
```
.
├── data                     # sample data set for the project
│   ├── log-data.zip         
│   └── song-data.zip        
├── dl.cfg                   # File with AWS credentials.
├── etl.py                   # Process to create dimension table and fact tables which loads data from s3 and upload user s3 bucket.
└── README.md                # Project Overview.
```

## ETL pipeline steps:

1. Load credentials (AWS Credentials)
2. Read data from S3
    - Song data: `s3://udacity-dend/song_data`
    - Log data: `s3://udacity-dend/log_data`

    The script reads song_data and load_data from S3.

3. Process data using spark

    Transforms them to create five different tables listed under `Dimension Tables and Fact Table`.

4. Load it back to S3

   Each of the five tables are written to parquet files in a separate analytics directory on S3. 

### Dimension Tables and Fact Table

```
# songplays (fact)
- songplay_id (INT) PRIMARY KEY: ID of each user song play 
- start_time (DATE) NOT NULL: Timestamp of beggining of user activity
- user_id (INT) NOT NULL: ID of user
- level (TEXT): User level {free | paid}
- song_id (TEXT) NOT NULL: ID of Song played
- artist_id (TEXT) NOT NULL: ID of Artist of the song played
- session_id (INT): ID of the user Session 
- location (TEXT): User location 
- user_agent (TEXT): Agent used by user to access Sparkify platform
```

```
users (dimension)
- user_id (INT) PRIMARY KEY: ID of user
- first_name (TEXT) NOT NULL: Name of user
- last_name (TEXT) NOT NULL: Last Name of user
- gender (TEXT): Gender of user {M | F}
- level (TEXT): User level {free | paid}
```
```
songs (dimension)
- song_id (TEXT) PRIMARY KEY: ID of Song
- title (TEXT) NOT NULL: Title of Song
- artist_id (TEXT) NOT NULL: ID of song Artist
- year (INT): Year of song release
- duration (FLOAT) NOT NULL: Song duration in milliseconds
```

```
artists (dimension)
- artist_id (TEXT) PRIMARY KEY: ID of Artist
- name (TEXT) NOT NULL: Name of Artist
- location (TEXT): Name of Artist city
- lattitude (FLOAT): Lattitude location of artist
- longitude (FLOAT): Longitude location of artist
```

```
time (dimension)
- start_time (DATE) PRIMARY KEY: Timestamp of row
- hour (INT): Hour associated to start_time
- day (INT): Day associated to start_time
- week (INT): Week of year associated to start_time
- month (INT): Month associated to start_time 
- year (INT): Year associated to start_time
- weekday (TEXT): Name of week day associated to start_time
```
