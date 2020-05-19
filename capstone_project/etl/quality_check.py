from pyspark.sql.functions import *


class QualityCheck:
    """
    Quality checks the dimension and fact tables.
    """

    def __init__(self, spark, paths):
        self.spark = spark
        self.paths = paths

    def demographics(self):
        """
        Get demographics dimension
        :return: demographics dimension
        """
        return self.spark.read.parquet(self.paths["demographics"])

    def airports(self):
        """
        Get airports dimension
        :return: airports dimension
        """
        return self.spark.read.parquet(self.paths["airports"])

    def temperature(self):
        """
        Get temperature dimension
        :return: temperature dimension
        """
        return self.spark.read.parquet(self.paths["temperature"])

    def get_facts(self):
        """
        Get facts table
        :return: facts table
        """
        return self.spark.read.parquet(self.paths["facts"])

    def get_dimensions(self):
        """
        Get all dimensions of the model
        :return: all dimensions
        """
        return self.demographics(), self.airports(), self.temperature()

    def row_count_check(self, dataframe):
        """
        empty dataframe check.
        :param dataframe: dataframe
        :return: true or false if the dataset has any row
        """
        return dataframe.count() > 0

    def integrity_checker(self, fact, dim_demographics, dim_airports, dim_temperature):
        """
        Check the integrity of the model. Checks if all the facts columns joined with the dimensions has correct values 
        :param fact: fact table
        :param dim_demographics: demographics dimension
        :param dim_airports: airports dimension
        :param dim_temperature: temperature dimension
        :return: true or false if integrity is correct.
        """
        integrity_demo = fact.select(col("cod_state")).distinct() \
                             .join(dim_demographics, fact["cod_state"] == dim_demographics["State_Code"], "left_anti") \
                             .count() == 0

        integrity_airports = fact.select(col("cod_port")).distinct() \
                                 .join(dim_airports, fact["cod_port"] == dim_airports["local_code"], "left_anti") \
                                 .count() == 0

        integrity_temperature = fact.select(col("cod_port")).distinct() \
                             .join(dim_temperature, fact["cod_port"] == dim_temperature["cod_port"], "left_anti") \
                             .count() == 0

        return integrity_demo & integrity_airports & integrity_temperature
