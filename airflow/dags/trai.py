import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.docker_operator import DockerOperator

from sklearn.linear_model import LogisticRegression
import mlflow
import mlflow.sklearn
import numpy as np


def setup_dat(*args, **kwargs):
    logging.info('Setting up data')
    return True

def get_data():
    X = np.array([-2, -1, 0, 1, 2, 1]).reshape(-1, 1)
    y = np.array([0, 0, 1, 1, 1, 0])

    return X, y

def trai_it(*args, **kwargs):
    logging.info('Training model')
    X, y = get_data()

    mlflow.set_tracking_uri('http://localhost:5001/')
    # experiment:
    # mlflow.set_experiment('airflow_model')

    with mlflow.start_run():
        alpha = 0.5
        l1_ratio = 0.5
        mlflow.log_param('alpha', alpha)
        mlflow.log_param('l1', l1_ratio)

        lr = LogisticRegression()
        lr.fit(X, y)

        mlflow.sklearn.log_model(sk_model=lr, artifact_path='model', conda_env={
         'channels': ['conda-forge'],
         'dependencies': [
             'python=3.7.0',
             'scikit-learn=0.19.2',
             'pysftp=0.2.9'
         ]
     })

    logging.info('Done traing and uploading')
    return True


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 1, 1),
    #'end_date': datetime(2017, 1, 2),
    'provide_context': True,
    'catchup': True
}

dag = DAG(dag_id='trainml-first', default_args=default_args, schedule_interval='@once')

with dag:
    setup_data = PythonOperator(task_id='setup_data_python', python_callable=setup_dat)
    train_model = PythonOperator(task_id='train_model', python_callable=trai_it)

setup_data >> train_model