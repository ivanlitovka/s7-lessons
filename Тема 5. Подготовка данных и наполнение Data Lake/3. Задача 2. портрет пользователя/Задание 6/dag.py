"""DAG для перекладки данных, расчёта кандидатов тегов и портрета пользователя."""

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
    'start_date': datetime.datetime(2022, 5, 31),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}


dag_spark = DAG(
    dag_id='sparkoperator',
    default_args=default_args,
    schedule_interval=None,
)


# ---- Задача 1: перекладка данных из Raw в ODS ----
events_partitioned = SparkSubmitOperator(
    task_id='events_partitioned',
    dag=dag_spark,
    application='/lessons/partition.py',
    conn_id='yarn_spark',
    application_args=[
        '2022-05-31',
        '/user/master/data/events',
        '/user/s1414928/data/events',
    ],
    conf={
        "spark.driver.maxResultSize": "20g",
    },
    executor_cores=2,
    executor_memory='2g',
)


# ---- Задача 2: расчёт кандидатов тегов (из предыдущего урока) ----
calculate_candidates = SparkSubmitOperator(
    task_id='calculate_candidates',
    dag=dag_spark,
    application='/lessons/verified_tags_candidates.py',
    conn_id='yarn_spark',
    application_args=[
        '2022-05-31',
        '7',
        '100',
        '/user/s1414928/data/events',
        '/user/master/data/snapshots/tags_verified/actual',
        '/user/s1414928/data/analytics/verified_tags_candidates_d7',
    ],
    conf={
        "spark.driver.maxResultSize": "4g",
    },
    executor_cores=2,
    executor_memory='4g',
)


# ---- Задача 3: портрет пользователя за 7 дней ----
user_interests_d7 = SparkSubmitOperator(
    task_id='user_interests_d7',
    dag=dag_spark,
    application='/lessons/user_interests.py',
    conn_id='yarn_spark',
    application_args=[
        '2022-05-31',
        '7',
        '/user/s1414928/data/events',
        '/user/s1414928/data/analytics/user_interests_d7',
    ],
    conf={
        "spark.driver.maxResultSize": "20g",
    },
    executor_cores=2,
    executor_memory='2g',
)


# ---- Задача 4: портрет пользователя за 28 дней ----
user_interests_d28 = SparkSubmitOperator(
    task_id='user_interests_d28',
    dag=dag_spark,
    application='/lessons/user_interests.py',
    conn_id='yarn_spark',
    application_args=[
        '2022-05-31',
        '28',
        '/user/s1414928/data/events',
        '/user/s1414928/data/analytics/user_interests_d28',
    ],
    conf={
        "spark.driver.maxResultSize": "20g",
    },
    executor_cores=2,
    executor_memory='2g',
)


events_partitioned >> [calculate_candidates,
                       user_interests_d7, user_interests_d28]
