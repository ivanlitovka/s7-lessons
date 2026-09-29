"""DAG. Перенесите свой DAG портрета пользователя и добавьте задачу портрета контактов."""

import datetime
import os

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME'] = '/usr'
os.environ['SPARK_HOME'] = '/usr/lib/spark'
os.environ['PYTHONPATH'] = '/usr/local/lib/python3.8'

default_args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2022, 5, 31),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

dag_spark = DAG(
    dag_id='sparkoperator',
    default_args=default_args,
    schedule_interval=None,
)

# ---- Портрет пользователя за 7 дней ----
user_interests_d7 = SparkSubmitOperator(
    task_id='user_interests_d7',
    dag=dag_spark,
    application='/lessons/user_interests.py',
    conn_id='yarn_spark',
    application_args=[
        '2022-05-25',
        '7',
        '/user/s1414928/data/events',
        '/user/s1414928/data/analytics/user_interests_d7',
    ],
    conf={"spark.driver.maxResultSize": "20g"},
    executor_cores=2,
    executor_memory='2g',
)


# ---- Портрет контактов за 7 дней ----
connection_interests_d7 = SparkSubmitOperator(
    task_id='connection_interests_d7',
    dag=dag_spark,
    application='/lessons/connection_interests.py',
    conn_id='yarn_spark',
    application_args=[
        '2022-05-25',
        '7',
        '/user/s1414928/data/events',
        '/user/s1414928/data/analytics/user_interests_d7',
        '/user/master/data/snapshots/tags_verified/actual',
        '/user/s1414928/data/analytics/connection_interests_d7',
    ],
    conf={"spark.driver.maxResultSize": "4g"},
    executor_cores=2,
    executor_memory='4g',
)


# ---- Зависимости ----
user_interests_d7 >> connection_interests_d7
