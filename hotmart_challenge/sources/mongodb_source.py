
from typing import List

from pyspark.sql import SparkSession

class MongoDBSource():
    """Class to extract from MongoDB sources.

    Args:
        spark_session (SparkSession): Spark session provided by the user.
        database_name (str): Database name.
        collection_name (str): Collection name's. Ex: Customers, etc

    """

    def __init__(
        self,
        spark_session: SparkSession,
        database_name: str = None,
        collection_name: str = None,
    ):
        self.spark_session = spark_session
        self.database_name = database_name
        self.collection_name = collection_name


    def execute(self):
        """Reads the source and load the mondodb data.

        """

        mongo_db_data_reader = self.spark_session.read
        mongo_db_read = (
            mongo_db_data_reader.format("mongodb")
            .option("database", self.database)
            .option("collection", self.collection_name)
            .load()
        )

       
        return mongo_db_read
