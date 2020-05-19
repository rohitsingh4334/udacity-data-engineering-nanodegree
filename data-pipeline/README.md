## Project: Data Pipeline with Apache Airflow.

### Introduction
A music streaming startup, Sparkify, has decide to automate their data pipelines and wants to load data from S3 data lake to AWS Redshift.

In this project, we will build an Airflow based etl pipeline that extracts their data from S3 based data lake and then load into AWS Redshift
into the form of dimension and fact tables (star schema).



### Project Structure
```
.
├── airflow                                                 # Root directory for project related airflow files.
│   ├── create_tables.sql                                   # DDL statement of AWS Redshift
│   ├── dags                                                # Custom dags 
│   │   ├── sparkify_dimesions_subdag.py
│   │   └── udac_example_dag.py
│   └── plugins                                             # customized helper and operators for airflow.
│       ├── helpers
│       │   ├── __init__.py
│       │   └── sql_queries.py
│       ├── __init__.py
│       └── operators
│           ├── create_table.py
│           ├── data_quality.py
│           ├── __init__.py
│           ├── load_dimension.py
│           ├── load_fact.py
│           └── stage_redshift.py
└── README.md                                               # Project overview and guidelines.
```