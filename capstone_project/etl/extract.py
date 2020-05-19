from pyspark.sql import SparkSession


class Extract:
    """
    extract data from the source and return the spark dataframe.
    """

    def __init__(self, spark, paths):
        self.spark = spark
        self.paths = paths

    def _get_standard_csv(self, filepath, delimiter=","):
        """
        extraction from csv file.
        :param filepath: .csv file path
        :param delimiter: delimiter
        :return: spark dataframe
        """
        return self.spark.read.format("csv").option("header", "true").option("delimiter", delimiter).load(filepath)

    def get_cities_demographics(self):
        return self._get_standard_csv(filepath=self.paths["us_cities_demographics"], delimiter=";")

    def get_airports_codes(self):
        return self._get_standard_csv(self.paths["airport_codes"])

    def get_immigration(self):
        return self.spark.read.parquet(self.paths["sas_data"])
    
    def get_temperature_data(self):
        return self._get_standard_csv(self.paths["temperature_data"])
