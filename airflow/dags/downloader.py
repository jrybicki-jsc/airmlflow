from datetime import datetime, timedelta
import logging
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from localutils import download_and_store, add_to_db, filter_file_list, generate_object_list, get_prefix_from_template as get_prefix, list_directory, print_db_stats, setup_daos

from tools import get_jsons_from_stream, split_record
from sqlalchemy import create_engine
from airflow.hooks.postgres_hook import PostgresHook


def update_last(**kwargs):
    prefix = get_prefix(**kwargs)
    target_dir = os.path.join(Variable.get('target_dir'), prefix)
    logging.info(f'Will be processing [{ target_dir }]')

    flist = list_directory(target_dir)
    logging.info(f'Files detected: { len(flist)}')

    previous_run = kwargs['prev_execution_date']
    next_run = kwargs['next_execution_date']
    #filtered_list = filter_file_list(
    #    flist=flist, previous_run=previous_run, next_run=next_run)
    filtered_list = list(flist)
    logging.info(
        f'Previous run was @{previous_run}, next will be @{next_run}. File list reduced to: {len(filtered_list)}')

    station_dao, series_dao, mes_dao = setup_daos()
    m = 0

    for fname in filtered_list:
        logging.info(f'Analyzing { fname}')

        with open(fname, 'rb') as f:
            for record in get_jsons_from_stream(stream=f, object_name=fname):
                station, measurement, _ = split_record(record)
                m += 1
                add_to_db(station_dao, series_dao, mes_dao, station=station,
                          measurement=measurement)

    logging.info(f'Number of measurements added to DB: {m}')
    print_db_stats(station_dao, series_dao, mes_dao)
    return True


def setup_new(*args, **kwargs):
    sql = '''select * from measurement where series_id=10261;'''
    db_url = kwargs['db_url']
    engine = create_engine(db_url, echo=True)
    conn = engine.connect()
    conn.execute('CREATE table IF NOT EXISTS data(x float, value float)')
    conn.execute('DELETE FROM data')

    try:
        pg = PostgresHook(postgres_conn_id='openaq-db')
        df = pg.get_pandas_df(sql, parameters=None)
        print(f'got the df: {df}')
        print(f'{df.columns}')

        for x, y in df['value'].iteritems():
            conn.execute(f'INSERT into data(x, value) values({x},{y} )')

        conn.close()
    except:
        logging.error(
            'Remote database not defined. Use [openaq-db] connection')
        return None


def test_data(*args, **kwargs):
    db_url = kwargs['db_url']
    logging.info(f'Checking data {db_url}')
    engine = create_engine(db_url, echo=True)
    conn = engine.connect()

    res = conn.execute('SELECT * from data')
    for row in res:
        print(f'{row}')

    conn.close()
    return True


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2014, 1, 2),
    'end_date': datetime(2014, 1, 2),
    'provide_context': True,
    'catchup': True
}

op_kwargs = {
    'base_dir': '/tmp/target/openaq/lists/',
    'db_url': 'sqlite:////tmp/mydata.db'
}

dag = DAG('downloader', default_args=default_args,
          schedule_interval=timedelta(days=1))

git_url = 'https://github.com/jrybicki-jsc/aflow.git'

with dag:
    get_objects_task = PythonOperator(task_id='get_object_list',
                                      python_callable=generate_object_list,
                                      op_kwargs=op_kwargs)
    download_task = PythonOperator(task_id='download',
                                   python_callable=download_and_store,
                                   op_kwargs=op_kwargs)
    db_task = PythonOperator(task_id='store_in_db',
                             python_callable=update_last,
                             op_kwargs=op_kwargs)

    setup_data = PythonOperator(task_id='setup_data_for_training',
                                python_callable=setup_new,
                                op_kwargs=op_kwargs)

    test_data = PythonOperator(task_id='test_training_data',
                               python_callable=test_data,
                               op_kwargs=op_kwargs)

    train_model = BashOperator(task_id='train_model_bash_chain',
                               bash_command=f'source /Users/jj/miniconda3/bin/../etc/profile.d/conda.sh && conda activate mlflow && mlflow run {git_url} -P alpha=0.4',
                               env={
                                   'MLFLOW_TRACKING_URI': 'http://localhost:5001/',
                                   'DB_URL': op_kwargs['db_url']
                               })

get_objects_task >> download_task >> db_task >> setup_data >> test_data >> train_model
