import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.bash_operator import BashOperator

from sklearn.linear_model import LogisticRegression
import mlflow
import mlflow.sklearn
import numpy as np


def setup_dat(*args, **kwargs):
    logging.info('Setting up data')
    return True


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 1, 1),
    #'end_date': datetime(2017, 1, 2),
    'provide_context': True,
    'catchup': True
}

dag = DAG(dag_id='trainml-bash', default_args=default_args, schedule_interval='@once')

with dag:
    setup_data = PythonOperator(task_id='setup2', python_callable=setup_dat)
    train_model = BashOperator(task_id='train_model_bash', bash_command='source /Users/jj/miniconda3/bin/../etc/profile.d/conda.sh && conda activate mlflow && mlflow run https://github.com/mlflow/mlflow-example -P alpha=0.4', env={'MLFLOW_TRACKING_URI':'http://localhost:5001/'})
    list_experiments = BashOperator(task_id='list_experiments', bash_command='source /Users/jj/miniconda3/bin/../etc/profile.d/conda.sh && conda activate mlflow && mlflow experiments list', env={'MLFLOW_TRACKING_URI':'http://localhost:5001/'})


setup_data >> list_experiments >> train_model
