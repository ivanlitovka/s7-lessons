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
    .appName("VerifiedTagsCandidatesD84") \
    .config("spark.driver.memory", "6g") \
    .config("spark.driver.maxResultSize", "4g") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "800") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.shuffleTracking.enabled", "true") \
    .config("spark.dynamicAllocation.minExecutors", "2") \
    .config("spark.dynamicAllocation.maxExecutors", "10") \
    .getOrCreate()

paths = input_paths("2022-05-31", 84)

messages = spark.read.parquet(*paths)

verified_tags = spark.read.parquet("/user/master/data/snapshots/tags_verified/actual")

all_tags = messages \
    .filter(F.col("event.message_channel_to").isNotNull()) \
    .filter(F.col("event.tags").isNotNull()) \
    .select(
        F.col("event.message_from").alias("user_id"),
        F.explode("event.tags").alias("tag")
    )

candidates = all_tags \
    .groupBy("tag") \
    .agg(F.countDistinct("user_id").alias("suggested_count")) \
    .filter(F.col("suggested_count") >= 100) \
    .join(verified_tags, ["tag"], "left_anti")


candidates.write \
    .mode("overwrite") \
    .parquet("/user/s1414928/data/analytics/candidates_d84_pyspark")