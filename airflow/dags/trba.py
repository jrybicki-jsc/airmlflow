import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from sqlalchemy import create_engine

def setup_dat(*args, **kwargs):
    #tdir = Variable.get('target_dir')
    db_url = kwargs['db_url']
    logging.info(f'Setting up data {db_url}')
    engine = create_engine(db_url, echo=True)
    conn = engine.connect()
    conn.execute('CREATE table IF NOT EXISTS data(x float, value float)')

    for i in range(-2, 2, 1):
        conn.execute(f'INSERT into data(x, value) values({i},{1 if i>0 else 0} )')
    conn.close()
    return True

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
    'start_date': datetime(2017, 1, 1),
    #'end_date': datetime(2017, 1, 2),
    'provide_context': True,
    'catchup': True
}

op_kwargs = {
    'db_url': 'sqlite:////tmp/mydata.db'
}

dag = DAG(dag_id='trainml-bash', default_args=default_args,
          schedule_interval='@once')

git_url = 'https://github.com/jrybicki-jsc/aflow.git'
#git_url = 'https://github.com/mlflow/mlflow-example'

with dag:
    setup_data = PythonOperator(task_id='setup2', python_callable=setup_dat, op_kwargs=op_kwargs)
    test_data = PythonOperator(task_id='test_dat', python_callable=test_data, op_kwargs=op_kwargs)
    train_model = BashOperator(task_id='train_model_bash',
                               bash_command=f'source /Users/jj/miniconda3/bin/../etc/profile.d/conda.sh && conda activate mlflow && mlflow run {git_url} -P alpha=0.4',
                               #bash_command='env',
                               env={
                                   'MLFLOW_TRACKING_URI': 'http://localhost:5001/',
                                   'DB_URL': op_kwargs['db_url']
                                   })
    list_experiments = BashOperator(task_id='list_experiments', bash_command='source /Users/jj/miniconda3/bin/../etc/profile.d/conda.sh && conda activate mlflow && mlflow experiments list', env={
                                    'MLFLOW_TRACKING_URI': 'http://localhost:5001/'})


setup_data >> test_data >> list_experiments >> train_model
