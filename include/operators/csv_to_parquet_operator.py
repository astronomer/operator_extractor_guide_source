import pandas as pd

from airflow.models.baseoperator import BaseOperator
from typing import List


class CsvToParquetOperator(BaseOperator):
    def __init__(
        self,
        path_to_csv: str,
        output_path: str,
        partition_by: List[str],
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.path_to_csv: str = path_to_csv
        self.output_path: str = output_path
        self.partition_by: List[str] = partition_by

    def execute(self):
    # Copies a file locally, for this to work with a remote source,
    # a conn_id must be supplied to the operator and the appropriate
    # hook used. See LocalFileSystemToS3Operator for an example:
    # https://registry.astronomer.io/providers/amazon/modules/localfilesystemtos3operator
        pd.read_csv(
            filepath_or_buffer=self.path_to_csv,
            header=0,
        ).to_parquet(
            path=self.output_path,
            partition_cols=self.partition_by
        )
        
