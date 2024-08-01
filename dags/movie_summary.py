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
    
    def fun_multi_y(ds_nodash):
        from mov.api.call import save2df
        p = {"multiMovieYn": "Y"}
        df = save2df(load_dt=ds_nodash, url_param=p)
        print(df.head(5))
    
    def fun_multi_n(ds_nodash):
        from mov.api.call import save2df
        p = {"multiMovieYn": "N"}
        df = save2df(load_dt=ds_nodash, url_param=p)
        print(df.head(5))

    def fun_nation_k(ds_nodash):
        from mov.api.call import save2df
        p = {"repNationCd": "K"}
        df = save2df(load_dt=ds_nodash, url_param=p)
        print(df.head(5))

    def fun_nation_f(ds_nodash):
        from mov.api.call import save2df
        p = {"repNationCd": "F"}
        df = save2df(load_dt=ds_nodash, url_param=p)
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
            return rm_dir.task_id
        else:
            return "get.start", "echo.task"


    get_data = PythonVirtualenvOperator(
        task_id="get_data",
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
    

    # 다양성 영화 유무
    multi_y = PythonVirtualenvOperator(
        task_id='multi.y',
        python_callable=fun_multi_y,
        requirements=["git+https://github.com/minju210/movie.git@0.3/api"],
        system_site_packages=False,
    )
    
    multi_n = PythonVirtualenvOperator(
        task_id='multi.n',
        python_callable=fun_multi_n,
        requirements=["git+https://github.com/minju210/movie.git@0.3/api"],
        system_site_packages=False,
    )
    
    # 한국외국영화
    nation_k = PythonVirtualenvOperator(
        task_id='nation.k',
        python_callable=fun_nation_k,
        requirements=["git+https://github.com/minju210/movie.git@0.3/api"],
        system_site_packages=False,
    )

    nation_f = PythonVirtualenvOperator(
        task_id='nation.f',
        python_callable=fun_nation_f,
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
    

    throw_err = BashOperator(
        task_id='throw.err',
        bash_command="exit 1",
        trigger_rule="all_done"
    )

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    get_start = EmptyOperator(
        task_id='get.start',
        trigger_rule="all_done"
    )
    get_end = EmptyOperator(task_id='get.end')


    start >> branch_op
    start >> throw_err >> save_data

    branch_op >> rm_dir >> get_start
    branch_op >> echo_task
    branch_op >> get_start
    get_start >> [get_data, multi_y, multi_n, nation_k, nation_f] >> get_end

    get_end >> save_data >> end
