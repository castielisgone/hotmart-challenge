from pyspark.sql import SparkSession

class RedShiftSink():
    """Class to sink into JDBC Redshit.

    Args:
        spark_session (SparkSession): Spark session provided by the user.
        schema (str, optional): Schema. Ex: schema
        database (str): Database name.
        dbtable (str): Table name's. Ex: table

    """

    def __init__(
        self,
        spark_session: SparkSession,
        database_dict: dict = None,
        schema: str = None,
        table_name: str = None,
        role: str = None,
        temp_dir: str = None,
        write_mode: str = None,
    ):
        self.spark_session = spark_session
        self.schema = schema
        self.table_name = table_name
        self.database_dict = database_dict
        self.role = role
        self.temp_dir = temp_dir
        self.write_mode = write_mode


    def execute(self):
        """Reads the source and execute the sink in database.
        """

        dbutils = self.dbutils(self.spark_session)

        secret_user = dbutils.secrets.get(
            scope="db-credentials", key=f"{self.database_dict[2]}_db_user"
        )

        secret_pass = dbutils.secrets.get(
            scope="db-credentials", key=f"{self.database_dict[2]}_db_password"
        )
        
        data = data.write

        data.format("io.github.spark_redshift_community.spark.redshift") \
            .option("url", f"""{self.url}?user={secret_user}&password={secret_pass}""") \
            .option("dbtable", f"""{self.schema}.{self.table_name}""") \
            .option("tempdir", self.temp_dir) \
            .option("aws_iam_role", self.role) \
            .mode(self.write_mode) \
            .save()

