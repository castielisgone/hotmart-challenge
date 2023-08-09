
from typing import List

from pyspark.sql import SparkSession

class JDBCSource():
    """Class to extract from JDBC sources.

    Args:
        component_pipeline_name (str): Name of this component in the pipeline.
        spark_session (SparkSession): Spark session provided by the user.
        schema (str, optional): Schema. Ex: schema
        database (str): Database name.
        dbtable (str): Table name's. Ex: table
        details (dict, optional): Dictionary containing extra details for the
            pipeline execution. Some execution flows need some specific configs to
            work correctly, like "temp_table_name" key for the SQLTransform use.
            Defaults to None.

    """

    def __init__(
        self,
        spark_session: SparkSession,
        database_dict: dict = None,
        schema: str = None,
        database_name: str = None,
        table_name: str = None,
        endpoint_selected: str = None,
    ):
        self.spark_session = spark_session
        self.schema = schema
        self.database = database_name
        self.dbtable = table_name
        self.endpoint_selected = endpoint_selected
        self.database_dict = database_dict


    def execute(self):
        """Reads the source and load the data.
        """

        jdbc_data_reader = self.spark_session.read
        dbutils = self.get_dbutils(self.spark_session)
        database_info = self.database_dict[self.database]

        secret_user = dbutils.secrets.get(
            scope="db-credentials", key=f"{database_info[2]}_db_user"
        )

        secret_pass = dbutils.secrets.get(
            scope="db-credentials", key=f"{database_info[2]}_db_password"
        )


        url = f"jdbc:postgresql://{database_info[0]}.{self.endpoint_selected}/{database_info[1]}"

        jbdc_data = (
            jdbc_data_reader.format("jdbc")
            .option("driver", "org.postgresql.Driver")
            .option("url", url)
            .option(
                "dbtable",
                f"{self.schema}.{self.dbtable}" if self.schema else f"{self.dbtable}",
            )
            .option("user", secret_user)
            .option("password", secret_pass)
            .load()
        )

        return jbdc_data
