"""Заготовка DAG. Создайте DAG и задачу запуска джобы из этого урока."""

import datetime
import os

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Настройка окружения
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME'] = '/usr'
os.environ['SPARK_HOME'] = '/usr/lib/spark'
os.environ['PYTHONPATH'] = '/usr/local/lib/python3.8'

default_args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2020, 1, 1),
}


dag_spark = DAG(
    dag_id='sparkoperator',
    default_args=default_args,
    schedule_interval=None,
)

# Задача
spark_submit_task = SparkSubmitOperator(
    task_id='spark_submit_task',
    dag=dag_spark,
    application='/lessons/partition.py',
    conn_id='yarn_spark',
    application_args=["2022-05-31", "/user/master/data/events", "/user/s1414928/data/events"],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores=2,
    executor_memory='2g',
)

spark_submit_task