# udacity-data-engineering-nanodegree
udacity nanodegree course projects.

### data warehousing project
```
.
├── capstone_project                                    # capstone project for udacity data engineering nanodegree.
│   ├── data
│   │   ├── etl
│   │   │   └── __pycache__
│   │   ├── output
│   │   │   ├── dimensions
│   │   │   │   ├── airports.parquet
│   │   │   │   ├── demographics.parquet
│   │   │   │   └── temperature.parquet
│   │   │   └── fact
│   │   │       └── immigrations_fact.parquet
│   │   │           └── arrival_year=2016
│   │   │               └── arrival_month=04
│   │   │                   ├── arrival_day=29
│   │   │                   └── arrival_day=30
│   │   ├── __pycache__
│   │   └── sas_data
│   └── temperature
│       └── data2
├── cloud-datawarehouse                                 # project solution of aws-redshift data modeling
├── data-model
│    ├── casssandra-data-model                          # project solution of cassandra data modeling
│    └── postgres-data-model                            # project solution of postgresql data modeling
├── data-pipeline                                       # project solution of data pipeline with apache airflow.
│   └── airflow
│       ├── dags
│       └── plugins
│           ├── helpers
│           └── operators
└── spark_data_lake                                     # project solution of data lake using spark and AWS S3.
    └── data
```