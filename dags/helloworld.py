from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
with DAG(
    'helloworld',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': True,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='hello world DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['helloworld'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = DummyOperator(
        task_id='rez.info')

    t2 = DummyOperator(task_id='result')

    t3 = DummyOperator(task_id='district')
    t4 = DummyOperator(task_id='facility.type')
    t5 = DummyOperator(task_id='by.day')
    task_start = DummyOperator(task_id='start')
    task_end = DummyOperator(task_id='end')
    
    t1 >> [t3, t4, t5] >> t2
    task_start >> t1
    t2 >> task_end

