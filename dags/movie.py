from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.dag import DAG
from airflow.operators.python import (
    #ExternalPythonOperator,
    PythonOperator,
    BranchPythonOperator,
    PythonVirtualenvOperator,
    #is_venv_installed,
)
with DAG(
    'movie',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': True,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='movie DAG',
    #schedule_interval=timedelta(days=1),
    schedule="10 2 * * *",
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=['movie', 'api', 'amt'],
) as dag:
    def get_data(ds_nodash):
        from mov.api.call import save2df
        df = save2df(ds_nodash)
        print(df.head(5))

    def save_data(ds_nodash):
        from mov.api.call import apply_type2df
        
        df = apply_type2df(load_dt=ds_nodash)
        print("*" * 33)
        print(df.head(10))
        print("*" * 33)
        print(df.dtypes)
        
        # 개봉일 기준 그룹핑 누적 관객수 합
        g = df.groupby('openDt')
        sum_df = g.agg({'audiCnt': 'sum'}).reset_index()
        print(sum_df)

    def print_context(ds=None, **kwargs):
        """Print the Airflow context and ds variable from the context."""
        print("::group::All kwargs")
        pprint(kwargs)
        print(kwargs)
        print("::endgroup::")
        print("::group::Context variable ds")
        print(ds)
        print("::endgroup::")
        return "Whatever you return gets printed in the logs"
    
    def branch_fun(ds_nodash):
        import os
        home_dir = os.path.expanduser("~")
        path = os.path.join(home_dir, f"tmp/test_parquet/load_dt={ds_nodash}")
        if os.path.exists(path):
            return rm_dir.task_id
            #return "rm.dir"
        else:
            return "get.data", "echo.task"
        
    branch_op = BranchPythonOperator(
        task_id="branch.op",
        python_callable=branch_fun
    )


    get_data = PythonVirtualenvOperator(
        task_id="get.data",
        trigger_rule="all_done",
        python_callable=get_data,
        requirements=["git+https://github.com/minju210/movie.git@0.3/api"],
        system_site_packages=False,
        #venv_cache_path="/home/minjoo/tmp2/air_venv/get_data"
    )

    save_data = PythonVirtualenvOperator(
        task_id="save.data",
        python_callable=save_data,
        system_site_packages=False,
        trigger_rule="one_success",
        requirements=["git+https://github.com/minju210/movie.git@0.3/api"],
        #venv_cache_path="/home/minjoo/tmp2/air_venv/get_data"
    )
    
    rm_dir = BashOperator(
        task_id='rm.dir',
        bash_command='rm -rf ~/tmp/test_parquet/load_dt={{ ds_nodash }}',
    )

    echo_task = BashOperator(
        task_id='echo.task',
        bash_command="echo 'task'"
    )

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    join_task = BashOperator(
            task_id='join',
            bash_command="exit 1",
            trigger_rule="all_done"
    )
    
    start >> branch_op
    start >> join_task >> save_data

    branch_op >> rm_dir >> get_data
    branch_op >> echo_task >> save_data
    branch_op >> get_data
    
    get_data >> save_data >> end
