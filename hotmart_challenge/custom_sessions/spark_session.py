
from pyspark.sql import SparkSession
from pyspark import SparkConf
from typing import List, Tuple


class SparkSession:
    """Custom session that initializes the Spark Session and Spark Context.

    Initializes the Spark Session using the defined configurations passed by the user
    or simply utilizes an already initialized Spark Session passed by the user.

    Also creates a class attribute to access the Spark Context object.
    """

    def __init__(
        self,
        app_name: str = "challenge_meli",
        configs: List[Tuple] = None,
        py_files: List[str] = None,
        spark_session: SparkSession = None,
    ):
        self.spark_session = spark_session
        configs = configs if configs else []
        py_files = py_files if py_files else []
        if not spark_session:
            spark_conf = SparkConf()
            spark_conf.setAll(configs if configs else [])
            self.spark_session = (
                SparkSession.builder.appName(app_name)
                .config(conf=spark_conf)
                .getOrCreate()
            )
            for file in py_files:
                self.spark_session.sparkContext.addPyFile(file)
        self.spark_context = self.spark_session.sparkContext