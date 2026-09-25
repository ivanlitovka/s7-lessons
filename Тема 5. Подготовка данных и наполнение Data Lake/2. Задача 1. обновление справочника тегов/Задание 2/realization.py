import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def input_paths(date, depth):
    end_date = datetime.datetime.strptime(date, "%Y-%m-%d").date()
    paths = []
    for i in range(depth):
        current_date = end_date - datetime.timedelta(days=i)
        date_str = current_date.strftime("%Y-%m-%d")
        paths.append(f"/user/s1414928/data/events/date={date_str}/event_type=message")
    return paths


spark = SparkSession.builder \
    .master("yarn") \
    .appName("VerifiedTagsCandidatesD7") \
    .getOrCreate()

# Пути за 7 дней
paths = input_paths("2022-05-31", 7)

# 1. Читаем сообщения
messages = spark.read.parquet(*paths)

# 2. Читаем verified_tags
verified_tags = spark.read.parquet("/user/master/data/snapshots/tags_verified/actual")

# 3. Разворачиваем теги
all_tags = messages \
    .filter(F.col("event.message_channel_to").isNotNull()) \
    .filter(F.col("event.tags").isNotNull()) \
    .select(
        F.col("event.message_from").alias("user_id"),
        F.explode("event.tags").alias("tag")
    )

# 4. Считаем уникальных пользователей по тегу
candidates = all_tags \
    .groupBy("tag") \
    .agg(F.countDistinct("user_id").alias("suggested_count")) \
    .filter(F.col("suggested_count") >= 100) \
    .join(verified_tags, ["tag"], "left_anti")

# 5. Записываем
candidates.write \
    .mode("overwrite") \
    .parquet("/user/s1414928/data/analytics/candidates_d7_pyspark")

