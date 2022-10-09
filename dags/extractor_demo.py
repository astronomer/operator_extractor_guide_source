from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.decorators import dag
from airflow.lineage.entities import Table, Column, File
from demo_util import MyPythonOperator
import pprint

default_args = {
    'owner': 'demo',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['demo@cosmicenergy.biz']
}

def python_operator(x):
    print(x)
    return "Hello"

# When using the DAG decorator, the "dag" argument doesn't need to be specified for each task.
# The "dag_id" value defaults to the name of the function it is decorating if not explicitly set.
# In this example, the "dag_id" value would be "example_dag_advanced".
@dag(
    # This DAG is set to run for the first time on June 11, 2021. Best practice is to use a static start_date.
    # Subsequent DAG runs are instantiated based on scheduler_interval below.
    start_date=days_ago(1),
    # This defines how many instantiations of this DAG (DAG Runs) can execute concurrently. In this case,
    # we're only allowing 1 DAG run at any given time, as opposed to allowing multiple overlapping DAG runs.
    # max_active_runs=1,
    # This defines how often your DAG will run, or the schedule by which DAG runs are created. It can be
    # defined as a cron expression or custom timetable. This DAG will run daily.
    schedule_interval=None,
    # Default settings applied to all tasks within the DAG; can be overwritten at the task level.
    default_args=default_args,
    default_view="graph", # This defines the default view for this DAG in the Airflow UI
    # When catchup=False, your DAG will only run for the latest schedule interval. In this case, this means
    # that tasks will not be run between June 11, 2021 and 1 day ago. When turned on, this DAG's first run
    # will be for today, per the @daily schedule interval
    catchup=False,
    tags=["demo"], # If set, this tag is shown in the DAG view of the Airflow UI
)
def extractor_demo():
    # DummyOperator placeholder for first task
    begin = DummyOperator(task_id="begin")
    # Last task will only trigger if no previous task failed
    end = DummyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    t1 = MyPythonOperator(
        task_id='test-operator',
        python_callable = python_operator,
        op_kwargs = {"x" : "Apache Airflow"},
        provide_context=True
    )

    begin >> t1 >> end

dag = extractor_demo()
