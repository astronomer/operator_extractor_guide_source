import logging, traceback
import pandas as pd
from typing import List, Optional

from openlineage.airflow.extractors.base import BaseExtractor, TaskMetadata
from openlineage.client.run import InputDataset, OutputDataset
from openlineage.common.dataset import SchemaField
from openlineage.client.facet import (
    SchemaDatasetFacet,
    DataSourceDatasetFacet,
    OwnershipJobFacet,
    OwnershipJobFacetOwners
)

log = logging.getLogger(__name__)

class CsvToParquetExtractor(BaseExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ["CsvToParquetOperator"]

    def extract(self) -> TaskMetadata:
        # Define task name as the dag_id and task_id for clarity.
        task_name = f"{self.operator.dag_id}.{self.operator.task_id}"
        # Define our TaskMetadata object that will be returned at the end.
        task_meta = TaskMetadata(
            name=task_name,
            inputs=[],
            outputs=[],
            run_facets={},
            job_facets={},
        )

        # Pull ownership information from the operator.
        ownership = OwnershipJobFacet(
            owners=[OwnershipJobFacetOwners(
                name=self.operator.owner,
                type="admin"
            )]
        )
        task_meta.job_facets["ownership": ownership]

        # Load our csv as a dataframe to do processing, we only need one data
        # row and the header to get what we need.
        # This may not be the most efficient method, but it's simple.
        df = pd.read_csv(
			filepath_or_buffer=self.operator.path_to_csv,
            header=0,
            nrows=1
        )

        # Create our input dataset. Output is created in extract_on_complete()
        # Use the filename as the dataset name with the correct extension.
        # The namespace is where the dataset resides, in this case it's a file
        # at a certain path.
        input = InputDataset(
            name=f"{task_name}.{self.operator.filename}.csv",
            namespace=f"csv://{self.operator.path_to_csv}"
        )

        # Generate the schema fields for the dataset to fill in the Info tab of the dataset.
        fields = []
        for (col, dtype) in zip(df.columns, df.dtypes):
            fields.append(SchemaField(name=col, type=dtype))

        # Create the SchemaDatasetFacet from the list of above-generated fields and add
        # it to the input dataset.
        schema = SchemaDatasetFacet(fields=fields)
        input.facets["schema"] = schema

        # Add the DataSourceDatasetFacet which is required for inputs and outputs
        # and displays in the Lineage UI when the dataset is clicked on.
        datasource = DataSourceDatasetFacet(
            name=f"{task_name}_csv_input", uri=f"csv://{self.operator.path_to_csv}"
        )
        input.facets["dataSource"] = datasource

        task_meta.inputs.append(input)

        return task_meta


    def extract_on_complete(self, task_instance) -> Optional[TaskMetadata]:
        task_meta = self.extract()

        df = pd.read_parquet(
			path=self.operator.output_path
        )

        # Create our output dataset.
        # Use the filename as the dataset name with the correct extension.
        # The namespace is where the dataset resides, in this case it's a file
        # at a certain path.
        output = OutputDataset(
            name=f"{task_meta.name}.{self.operator.filename}.parquet",
            namespace=f"parquet://{self.operator.output_path}"
        )

        # Generate the schema fields for the dataset to fill in the Info tab of the dataset.
        fields = []
        for (col, dtype) in zip(df.columns, df.dtypes):
            fields.append(SchemaField(name=col, type=dtype))

        # Create the SchemaDatasetFacet from the list of above-generated fields and add
        # it to the input dataset.
        schema = SchemaDatasetFacet(fields=fields)
        output.facets["schema"] = schema

        # Add the DataSourceDatasetFacet which is required for inputs and outputs
        # and displays in the Lineage UI when the dataset is clicked on.
        datasource = DataSourceDatasetFacet(
            name=f"{task_meta.name}_parquet_output",
            uri=f"parquet://{self.operator.output_path}"
        )
        input.facets["dataSource"] = datasource

        task_meta.outputs.append(output)

        return task_meta
