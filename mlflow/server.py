import mlflow
from sklearn.linear_model import LogisticRegression
import mlflow.sklearn
import numpy as np
import sys


if __name__ == "__main__":
   print('Starting up...')
   mlflow.set_tracking_uri('http://localhost:5001/')

   with mlflow.start_run():
     mlflow.log_param("x", 1)
     mlflow.log_metric("y", 2)

     # metrics:
     for i in range(0, 3):
        mlflow.log_metric(key='quality', value=2*i, step=i)

     lr = LogisticRegression()
     X = np.array([-2, -1, 0, 1, 2, 1]).reshape(-1, 1)
     y = np.array([0, 0, 1, 1, 1, 0])
     lr.fit(X, y)

     mlflow.sklearn.log_model(sk_model=lr, artifact_path='model', conda_env={
         'name': 'mlflow-env',
         'channels': ['conda-forge'],
         'dependencies': [
             'python=3.7.0',
             'scikit-learn=0.19.2',
             'pysftp=0.2.9'
         ]
     })
