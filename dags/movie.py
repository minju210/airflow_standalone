from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
with DAG(
    'movie',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': True,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=5)
    },
    description='movie DAG',
    #schedule_interval=timedelta(days=1),
    schedule="10 2 * * *",
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=['movie', 'api', 'amt'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators


    task_get_data = BashOperator(
        task_id="get.data",
        bash_command='date'
    )

    task_save_data = BashOperator(
        task_id="save.data",
        bash_command='date'
    )
    
#    task_err = BashOperator(
#        task_id="err.report",
#        bash_command="""
#            echo "err report"
#        """,
#        trigger_rule="one_failed"
#    )

    task_start = EmptyOperator(task_id='start')
    task_end = EmptyOperator(task_id='end')
    
    task_start >> task_get_data >> task_save_data >> task_end
