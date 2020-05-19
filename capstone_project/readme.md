## Data Engineering Capstone Project 
#### Project Summary
<p>The project's goal is to build out an ETL pipeline that uses I94 immagratin data and city tempature data to create a database that is optimized for queries to analize immagration events. The database will then be used to answer questions regarding immagration behavior to location tempatures, airports details and demographics of the cities.</p>


**Project Structure**
```
├── etl                                      # etl scripts of the project.
│   ├── data_cleaning.py                     # data cleaning.
│   ├── extract.py                           # data extraction.
│   ├── models.py                            # script to convert dataframe into parquert.
│   ├── quality_check.py                     # script for quality checks.
│   └── transform.py                         # data transformation script
├── output                                   # outcome of the project
│   ├── dimensions                           # dimension tables
│   │   ├── airports.parquet                 # airport dimension
│   │   ├── demographics.parquet             # demographics dimension
│   │   └── temperature.parquet              # temperature dimension
│   └── fact                                 # fact tables
│       └── immigrations_fact.parquet        # immigration fact table.
├── sas_data                                 # I94 Immigration Data data
├── us-cities-demographics.csv               # US City Demographics
├── valid_codes.txt                          # valid I94port
├── readme.md                                # Project Overview/ Instructions
├── airport-codes_csv.csv                    # table of airport codes and corresponding cities
├── Capstone Project Template.ipynb          # project template notebook 
├── I94_SAS_Labels_Descriptions.SAS          # I94 Immigration Data data decriptions.
├── immigration_data_sample.csv              # sample data of immigrations.
└── main.py                                  # main file for project.
```


**How to run:**
```
python main.py
```

**Notebook for exploration**
Capstone Project Template.ipynb