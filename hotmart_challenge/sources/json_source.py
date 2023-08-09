
from typing import List
from pyspark.sql import SparkSession

class JsonSource():
    """Class to extract from Json sources.
    Args:
        spark_session (SparkSession): Spark session provided by the user.
        source_files (List[str], optional): Source files full path name. Can be one
            file only or multiple ones, as long as all the paths contain data with
            schemas that don't conflict with each other. Defaults to None.
        base_path (str, optional): The source file base path, used for partitioned
            datasets, where passing the base path enables the partition column(s)
            to be used on the dataframe. Example: a folder called "/raw/data/"
            contains the data to be read and only the folder of the date
            "2021-01-01" is needed, so the `source_file` would be
            "/raw/data/date=2022-01-01" and `base_path` would be "/raw/data/".
            If the user decides to pass the start or end date, the base path is
            necessary to indicate the base folder to search for the partitions.
            Defaults to None.
        start_date (str, optional): Start date from where to start reading the data
            based on partitioning. If this parameter is passed, the list of source
            files is ignored and overwritten with the source files starting from
            this date. Defaults to None.
        end_date (str, optional): End date from where to end reading the data based
            on partitioning. If this parameter is passed, the list of source
            files is ignored and overwritten with the source files ending to
            this date. Defaults to None.
        date_partition (str, optional): Name of the date partition of the source
            data. Usually it's a prefix like "date=". Defaults to "date=".
        read_options (dict, optional): JSON read options, like inferSchema, multiLine
        recursiveFileLookup and etc. Defaults to None.
        use_base_path (bool, optional): If True, applies the "basePath" options to the
            spark reader. Defaults to True.
    """

    def __init__(
        self,
        spark_session: SparkSession,
        source_files: List[str] = None,
        base_path: str = None,
        start_date: str = None,
        end_date: str = None,
        schema: str = None,
        read_options: dict = None,
        date_partition: str = "date=",
        use_base_path: bool = True,
        details: dict = None,
    ):
        super().__init__(
            spark_session,
            source_files,
            base_path,
            start_date,
            end_date,
            schema,
            read_options,
            date_partition,
            use_base_path,
            details,
            "json",
        )

    def load(self, **kwargs):
        self.source_files = kwargs["source_files_result"]

    def execute(self):
        """Reads the source into json data and return it.
        """

        data_reader = self.spark_session.read

        if "inferSchema" not in self.read_options:
            self.read_options["inferSchema"] = "false"

        if "recursiveFileLookup" not in self.read_options:
            self.read_options["recursiveFileLookup"] = "false"

        if "multiLine" not in self.read_options:
            self.read_options["multiLine"] = "false"


        if self.base_path and self.use_base_path:
            data_reader = data_reader.option("basePath", self.base_path).options(
                **self.read_options
            )

        if self.schema:
            data_reader = data_reader.schema(self.schema)

        if len(self.source_files) > 0:
            json_data = data_reader.json(self.source_files)
    

        return json_data
