import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Создаём Spark-сессию
spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()

# 2. Читаем весь каталог снапшота channels (а не один part-файл!)
df = spark.read.parquet("/user/master/data/snapshots/channels/actual")

# 3. Сохраняем с партиционированием по channel_type и режимом append
df.write \
  .mode("append") \
  .partitionBy("channel_type") \
  .parquet("/user/s1414928/analytics/test")

# 4. Читаем обратно и выводим уникальные типы каналов
df2 = spark.read.parquet("/user/s1414928/analytics/test")

df2.select("channel_type") \
   .orderBy("channel_type") \
   .distinct() \
   .show()
