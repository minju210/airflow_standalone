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
    tags=['movie', 'api', 'amt'],
) as dag:
    def get_data(ds, **kwargs):
        print(ds)
        print(kwargs)
        from mov.api.call import gen_url, req, get_key, req2list, list2df, save2df
        print(f"ds_nodash => {kwargs['ds_nodash']}")
        key = get_key()
        print(f"MOVIE_API_KEY => {key}")
        date = kwargs['ds_nodash']
        df = save2df(date)
        print(df.head(5))

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
    
    def save_data(ds_nodash):
        from mov.api.call import apply_type2df
        df = apply_type2df(load_dt=ds_nodash)

        g = df.groupby('openDt')
        sum_df = g.agg({'audiCnt': 'sum'}).reset_index()
        print(sum_df)

    def branch_fun(ds_nodash):
        import os
        home_dir = os.path.expanduser("~")
        path = os.path.join(home_dir, f"tmp/test_parquet/load_dt={ds_nodash}")

       #if os.path.exists(f'~/tmp/test_parquet/load_dt={ld}')
        if os.path.exists(path):
            return "rm.dir"
        else:
            return "get.data", "echo.task"


    get_data = PythonVirtualenvOperator(
        task_id="get_data",
        trigger_rule="all_done",
        python_callable=get_data,
        requirements=["git+https://github.com/minju210/movie.git@0.3/api"],
        system_site_packages=False,
    )

    save_data = PythonVirtualenvOperator(
        task_id='save.data',
        trigger_rule="one_success",
        python_callable=save_data,
        requirements=["git+https://github.com/minju210/movie.git@0.3/api"],
        system_site_packages=False,
    )
    
    branch_op = BranchPythonOperator(
        task_id="branch.op",
        python_callable=branch_fun,
    )

    rm_dir = BashOperator(
        task_id='rm.dir',
        bash_command='rm -rf ~/tmp/test_parquet/load_dt={{ ds_nodash }}'
    )
    
    echo_task = BashOperator(
        task_id="echo.task",
        bash_command="echo 'task'"
    )
    

    join_task = BashOperator(
        task_id='join',
        bash_command="exit 1",
        trigger_rule="all_done"
    )

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    multi_y = EmptyOperator(task_id='multi.y')
    multi_n = EmptyOperator(task_id='multi.n')
    nation_k = EmptyOperator(task_id='nation.k')
    nation_f = EmptyOperator(task_id='nation.f')


    start >> branch_op
    start >> join_task >> save_data

    branch_op >> rm_dir >> [get_data, multi_y, multi_n, nation_k, nation_f]
    branch_op >> echo_task >> save_data 
    branch_op >> [get_data, multi_y, multi_n, nation_k, nation_f]

    [get_data, multi_y, multi_n, nation_k, nation_f] >> save_data >> end
