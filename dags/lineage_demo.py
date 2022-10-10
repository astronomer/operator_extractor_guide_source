from datetime import datetime
from openlineage.airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.decorators import dag
from airflow.utils.task_group import TaskGroup


@dag(
    start_date=days_ago(1),
    max_active_runs=1,
    schedule_interval=None,
    default_view="graph",
    catchup=False,
    tags=["demo"],
)
def lineage_demo():

    with TaskGroup("table_init") as table_init:
        init_course_r = PostgresOperator(
            task_id="init_course_r",
            postgres_conn_id="demo_postgres_db",
            sql="""
            CREATE TABLE IF NOT EXISTS COURSE_REGISTRATIONS (
                CREATE_DATE                 DATE,
                COURSE_ID                   NUMBER,
                COURSE_NAME                 TEXT,
                SIGNUP_URL                  TEXT,
                NUM_STUDENTS                NUMBER,
                TOTAL_REVENUE               NUMBER
            );
            """,
        )
        init_course_r

    with TaskGroup("course_etl") as course_etl:
        insert_course_r = PostgresOperator(
            task_id="insert_course_c",
            postgres_conn_id="demo_postgres_db",
            sql="""
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
            """,
        )
        insert_course_r

    table_init >> course_etl


dag = lineage_demo()
