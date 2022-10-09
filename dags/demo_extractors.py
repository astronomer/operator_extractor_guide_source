import logging, traceback
from typing import List, Optional

from openlineage.airflow.extractors.base import (
    BaseExtractor,
    TaskMetadata
)

from openlineage.client.run import InputDataset, OutputDataset
from openlineage.common.dataset import SchemaField
from openlineage.client.facet import (
    SchemaDatasetFacet, 
    DataSourceDatasetFacet, 
    ErrorMessageRunFacet
)

log = logging.getLogger(__name__)

class MyPythonExtractor(BaseExtractor):
    def __init__(self, operator):
        super().__init__(operator)
    
    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['MyPythonOperator']
    
    def extract(self) -> TaskMetadata:
        task_name = f"{self.operator.dag_id}.{self.operator.task_id}"

        task_meta = TaskMetadata(
                        name=task_name,
                        inputs=[],
                        outputs=[],
                        run_facets={},
                        job_facets={},
                    )
        try:
            # convert all args and kwargs into map
            all_args = {("arg" + str(idx + 1)): arg for idx, arg in enumerate(self.operator.op_args)}
            all_args.update(self.operator.op_kwargs)

            input = InputDataset(name=f"{task_name}_python_input", namespace="python://args")

            fields = []
            for key, value in all_args.items():
                fields.append(SchemaField(name=key, type="VARCHAR", description=value))

            # schema will contain key and variable of arguments
            schema = SchemaDatasetFacet(fields=fields)
            input.facets['schema'] = schema
            task_meta.inputs.append(input)

            # datasource which is one of the facets required
            datasource = DataSourceDatasetFacet(name=f"{task_name}_python_input", uri='python://args')
            input.facets['dataSource'] = datasource

            return task_meta
        except Exception as e:
            log.exception("Exception has occurred in extract()")
            error = ErrorMessageRunFacet(str(e), 'python')
            error.stackTrace = traceback.format_exc()
            task_meta.run_facets['errorMessage'] = error
            raise Exception(e)
        finally:
            return task_meta

    def extract_on_complete(self, task_instance) -> Optional[TaskMetadata]:
        task_meta_data = self.extract()
        task_name = f"{self.operator.dag_id}.{self.operator.task_id}"
        if self.operator.do_xcom_push is True:
            log.debug("do_xcom_push is True")
            start_date = self.operator.start_date       # execution start
            end_date = self.operator.end_date           # execution end date
            task_id = self.operator.task_id
            dag_id = self.operator.dag_id
            ti = self.operator.get_task_instances(start_date=start_date,end_date=end_date)
            if len(ti) > 0:
                log.debug(f"> ti for operator size is {len(ti)}")
                task_instance = ti[len(ti)-1]
                return_value = task_instance.xcom_pull(task_ids=task_id, dag_id=dag_id, key='return_value')
                if return_value is not None:
                    log.debug(f"return value received: {return_value}")
                    # populate output variable to output
                    output = OutputDataset(name=f"{task_name}_python_output", namespace="python://args")
                    fields = []
                    fields.append(SchemaField(name="return_value", type="VARCHAR", description=str(return_value)))
                    # schema will contain key and variable of arguments
                    schema = SchemaDatasetFacet(fields=fields)
                    output.facets['schema'] = schema
                    datasource = DataSourceDatasetFacet(name=f"{task_name}_python_output", uri='python://args')
                    output.facets['dataSource'] = datasource
                    task_meta_data.outputs.append(output)
        return task_meta_data
