from datetime import datetime
from openlineage.airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.decorators import dag
from airflow.utils.task_group import TaskGroup

default_args = {
    'owner': 'registry',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False
}

# When using the DAG decorator, the "dag" argument doesn't need to be specified for each task.
# The "dag_id" value defaults to the name of the function it is decorating if not explicitly set.
# In this example, the "dag_id" value would be "example_dag_advanced".
@dag(
    # This DAG is set to run for the first time on June 11, 2021. Best practice is to use a static start_date.
    # Subsequent DAG runs are instantiated based on scheduler_interval below.
    start_date=days_ago(1),
    # This defines how many instantiations of this DAG (DAG Runs) can execute concurrently. In this case,
    # we're only allowing 1 DAG run at any given time, as opposed to allowing multiple overlapping DAG runs.
    max_active_runs=1,
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
def lineage_demo():

    with TaskGroup("table_init") as table_init:
        init_course_r = PostgresOperator(
            task_id='init_course_r',
            postgres_conn_id='demo_postgres_db',
            sql='''
            CREATE TABLE IF NOT EXISTS COURSE_REGISTRATIONS (
                CREATE_DATE                 DATE,
                COURSE_ID                   NUMBER,
                COURSE_NAME                 TEXT,
                SIGNUP_URL                  TEXT,
                NUM_STUDENTS                NUMBER,
                TOTAL_REVENUE               NUMBER
            );
            '''
        )
        init_course_r

    with TaskGroup("course_etl") as course_etl:
        insert_course_r = PostgresOperator(
            task_id='insert_course_c',
            postgres_conn_id='demo_postgres_db',
            sql='''
            INSERT INTO COURSE_REGISTRATIONS(CREATE_DATE, COURSE_ID, COURSE_NAME, SIGNUP_URL, NUM_STUDENTS, TOTAL_REVENUE)
                SELECT
                    CREATE_DATE
                    , COURSE_ID
                    , COURSE_NAME
                    , SIGNUP_URL
                    , NUM_STUDENTS
                    , TOTAL_REVENUE
                FROM 
                    tmp_course_registrations;
            '''
        )
        insert_course_r

    table_init >> course_etl

dag = lineage_demo()
