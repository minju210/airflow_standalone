from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

def gen_emp(id, rule="all_success"):
    op = EmptyOperator(task_id=id, trigger_rule=rule)
    return op

with DAG(
    'make_parquet',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    max_active_runs=1,
    max_active_tasks=2,
    description='history log 2 mysql db',
    #schedule_interval=timedelta(days=1),
    schedule="10 4 * * *",
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['simple', 'etl', 'bash', 'db', 'history', 'parquet'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators

    task_check_done = BashOperator(
        task_id="check.done",
        bash_command="""
            DONE_FILE={{ var.value.IMPORT_DONE_PATH }}/{{ds_nodash}}/_DONE
            bash {{ var.value.CHECK_SH }} $DONE_FILE
        """
    )
    
    task_to_parquet = BashOperator(
        task_id="to.parquet",
        bash_command="""
            echo "to.parquet"
            
            READ_PATH="~/data/csv/{{ds_nodash}}/csv.csv"
            SAVE_PATH=~/data/parquet
            python ~/airflow/py/csv2parquet.py $READ_PATH $SAVE_PATH
        """
    )

    task_make_done = BashOperator(
        task_id="make.done",
        bash_command="""
            echo "make.done.start"

            DONE_PATH={{ var.value.IMPORT_DONE_PATH }}/{{ ds_nodash }}
            mkdir -p ${DONE_PATH}
            touch ${DONE_PATH}/_DONE
        """
    )

    task_err = BashOperator(
        task_id="err.report",
        bash_command="""
            echo "err report"
        """,
        trigger_rule="one_failed"
    )

    task_start = gen_emp('start')
    task_end = gen_emp('end', 'all_done')
    
    task_start >> task_check_done >> task_to_parquet >> task_make_done >> task_end
    task_check_done >> task_err >> task_end
