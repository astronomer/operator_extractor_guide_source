from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.decorators import dag
from airflow.lineage.entities import Table, Column, File
from include.operators.demo_operator import MyPythonOperator
import pprint


def python_operator(x):
    print(x)
    return "Hello"


# When using the DAG decorator, the "dag" argument doesn't need to be specified for each task.
# The "dag_id" value defaults to the name of the function it is decorating if not explicitly set.
# In this example, the "dag_id" value would be "example_dag_advanced".
@dag(
    start_date=days_ago(1),
    schedule_interval=None,
    default_view="graph",
    catchup=False,
    tags=["demo"],  # If set, this tag is shown in the DAG view of the Airflow UI
)
def extractor_demo():
    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    t1 = MyPythonOperator(
        task_id="test-operator",
        python_callable=python_operator,
        op_kwargs={"x": "Apache Airflow"},
        provide_context=True,
    )

    begin >> t1 >> end


dag = extractor_demo()
