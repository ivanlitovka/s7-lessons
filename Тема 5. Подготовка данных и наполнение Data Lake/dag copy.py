"""DAG. Перекладка данных, кандидаты тегов, портрет пользователя."""

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


# ---- 1. Перекладка данных из Raw в ODS ----
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


# ---- 2. Кандидаты тегов за 7 дней ----
verified_tags_candidates_d7 = SparkSubmitOperator(
    task_id='verified_tags_candidates_d7',
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


# ---- 3. Кандидаты тегов за 84 дня ----
verified_tags_candidates_d84 = SparkSubmitOperator(
    task_id='verified_tags_candidates_d84',
    dag=dag_spark,
    application='/lessons/verified_tags_candidates.py',
    conn_id='yarn_spark',
    application_args=[
        '2022-05-31',
        '84',
        '100',
        '/user/s1414928/data/events',
        '/user/master/data/snapshots/tags_verified/actual',
        '/user/s1414928/data/analytics/verified_tags_candidates_d84',
    ],
    conf={
        "spark.driver.maxResultSize": "4g",
    },
    executor_cores=2,
    executor_memory='4g',
)


# ---- 4. Портрет пользователя за 7 дней ----
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


# ---- 5. Портрет пользователя за 28 дней ----
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


# ---- Зависимости ----
events_partitioned >> [verified_tags_candidates_d7, verified_tags_candidates_d84, user_interests_d7, user_interests_d28]