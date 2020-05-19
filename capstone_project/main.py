# all imports
from pyspark.sql import SparkSession, SQLContext, GroupedData
from pyspark.sql.functions import *
from glob import glob
import pandas as pd
from etl.extract import Extract
import re
from etl.data_cleaning import Cleaner
from etl.models import Model
from etl.transform import Transformer
from etl.quality_check import QualityCheck


def main(spark, input_path, output_path):
    # data extraction from various sources.
    extract = Extract(spark, input_path)
    demographics = extract.get_cities_demographics()
    airport_codes = extract.get_airports_codes()
    immigration_data = extract.get_immigration()
    temperature_data = extract.get_temperature_data()

    # data cleaning step.
    demographics = Cleaner.get_cities_demographics(demographics)
    airport_codes = Cleaner.get_airports(airport_codes)
    immigration_data = Cleaner.get_immigration(immigration_data)
    temperature_data = Cleaner.get_temperature(temperature_data)


    # data transformation step.
    demographics = Transformer.demographics(demographics)
    immigration_data = Transformer.immigrants(immigration_data)
    temperature_data = Transformer.temperatue(temperature_data)

    # data modeling step
    model = Model(spark, output_path)
    model.modelize(immigration_data, demographics, airport_codes, temperature_data)

    # quality checks
    checker = QualityCheck(spark, output_path)
    immigration_fact = checker.get_facts()
    dim_demographics, dim_airports, dim_temperature = checker.get_dimensions()

    print("Demogrhaics DIM is not empty: {0}".format(checker.row_count_check(dim_demographics)))
    print("Airports DIM is not empty: {0}".format(checker.row_count_check(dim_airports)))
    print("Temperature DIM is not empty: {0}".format(checker.row_count_check(dim_temperature)))
    print("Immigration DIM is not empty: {0}".format(checker.row_count_check(immigration_fact)))
    # data integretiy check
    print('Integrity Check: {0}'.format(checker.integrity_checker(immigration_fact, dim_demographics, dim_airports, dim_temperature)))
    
    print('Final Data Dictionary: ')
    print('demographics dim: ')
    print(dim_demographics.printSchema())
    print('airports dim: ')
    print(dim_airports.printSchema())
    print('temperature dim: ')
    print(dim_temperature.printSchema())
    print('immigration fact: ')
    print(immigration_fact.printSchema())


if __name__ == '__main__':
    print('process starting.....')
    # create Spark Session
    spark = SparkSession.builder.\
    config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11").\
    config("spark.sql.broadcastTimeout", "36000").\
    enableHiveSupport().getOrCreate()
    
    # data extraction path.
    input_path = {
        "us_cities_demographics" : "us-cities-demographics.csv",
        "airport_codes" :  "airport-codes_csv.csv",
        "sas_data" : "sas_data/",
        "temperature_data": '/data2/GlobalLandTemperaturesByCity.csv'
    }
    
    # final outputs
    output_path = {
        'demographics': './output/dimensions/demographics.parquet',
        'airports': './output/dimensions/airports.parquet',
        'temperature': './output/dimensions/temperature.parquet',
        'facts': './output/fact/immigrations_fact.parquet'
    }
    main(spark, input_path, output_path)
    print('process finished.....')