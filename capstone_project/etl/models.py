from pyspark.sql import SparkSession, SQLContext, GroupedData
from pyspark.sql.functions import *
from pyspark.sql.functions import date_add as d_add
from pyspark.sql.types import DoubleType


class Model:
    """
    Creating the facts table and dimension tables for datawarehousing in the form of star schema.
    """

    def __init__(self, spark, paths):
        self.spark = spark
        self.paths = paths

    def demographics(self, demographics_df):
        """
        Create de demographics dimension table in parquet.
        :param demographics_df: demographics dataset.
        """
        demographics_df.write.mode('overwrite').parquet(self.paths["demographics"])

    def airports(self, airports_df):
        """
        Create de airports dimension table in parquet.
        :param airports_df: airports dataset
        """
        airports_df.write.mode('overwrite').parquet(self.paths["airports"])

    def temperature(self, temperature_df):
        """
        Create de airports temperature table in parquet.
        :param temperature_df: temperature dataset
        """
        temperature_df.write.mode('overwrite').parquet(self.paths["temperature"])

    def facts(self, facts):
        """
        Create facts table from immigration in parquet particioned by arrival_year, arrival_month and arrival_day
        :param facts: immigration dataset
        """
        facts.limit(10000).write.partitionBy("arrival_year", "arrival_month", "arrival_day").mode('overwrite').parquet(
            self.paths["facts"])

    def modelize(self, facts, dim_demographics, dim_airports, dim_temperature):
        """
        Create the Star Schema for the Data Warwhouse
        :param facts: facts table, inmigration dataset
        :param dim_demographics: dimension demographics
        :param dim_airports: dimension airports
        :param dim_temperature: dimension temperature
        """
        facts = facts \
            .join(broadcast(dim_demographics), facts["cod_state"] == dim_demographics["State_Code"], "left_semi") \
            .join(broadcast(dim_airports), facts["cod_port"] == dim_airports["local_code"], "left_semi") \
            .join(broadcast(dim_temperature), facts["cod_port"] == dim_temperature["cod_port"], "left_semi")
        
        print("writing demographics parquet....")
        self.demographics(dim_demographics)
        print("writing airports parquet....")
        self.airports(dim_airports)
        print("writing temperature parquet....")
        self.temperature(dim_temperature)
        print("writing facts parquet....")
        self.facts(facts)
