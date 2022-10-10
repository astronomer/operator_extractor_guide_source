from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.decorators import dag
from airflow.lineage.entities import Table, Column, File
import pprint


def python_operator(x):
    print(x)

def create_table(cluster, database, name):
    return Table(
        cluster=cluster,
        database=database,
        name=name
    )

def create_file(url):
    return File(
        url=url
    )

@dag(
    start_date=days_ago(1),
    schedule_interval=None,
    default_view="graph",
    catchup=False,
    tags=["demo"],
)
def inlet_outlet_demo():
    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    # - setting up input table
    input_table = Table(cluster="my_cluster_1", database="temp", name="input_data-1")
    input_table.columns.append(Column(name="id", description="id of the data", data_type="integer"))
    input_table.columns.append(Column(name="data", description="content of the data", data_type="varchar"))

    # - setting up output table
    output = Table(cluster="my_cluster_1", database="op", name="op_table-1")
    output.columns.append(Column(name="id", description="id of the data", data_type="integer"))
    output.columns.append(Column(name="analyzed", description="analyzed result", data_type="varchar"))
    output.columns.append(Column(name="analyzed_dt", description="timestamp of the analysis", data_type="timestamp"))

    t1 = PythonOperator(
        task_id='test-operator',
        python_callable = python_operator,
        op_kwargs = {"x" : "Apache Airflow"},
        inlets=[input_table],
        outlets=[output]
    )

    begin >> t1 >> end

dag = inlet_outlet_demo()
