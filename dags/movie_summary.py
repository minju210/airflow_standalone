from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
        PythonOperator,
        PythonVirtualenvOperator,
        BranchPythonOperator
        )

# import func
#from movie.api.call import gen_url, req, get_key, req2list, list2df, save2df

with DAG(
    'movie',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    max_active_runs=1,
    max_active_tasks=3,
    description='movie DAG',
    #schedule_interval=timedelta(days=1),
    schedule="10 2 * * *",
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=['movie', 'api', 'summary'],
) as dag:

    task_start = EmptyOperator(task_id='start')
    task_end = EmptyOperator(task_id='end')

    apply_type = EmptyOperator(
            task_id = 'apply.type',
            )

    merge_df = EmptyOperator(
            task_id = 'merge.df',
            )

    de_dup = EmptyOperator(
            task_id = 'de.dup',
            )

    summary_df = EmptyOperator(
            task_id = 'summary.df',
            )
    task_start >> apply_type >> merge_df >> de_dup >> summary_df >> task_end
