import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()
events = spark.read.json("/user/master/data/events/date=2022-05-31").cache()
# events.select("event.*").show(10, False)
# events_curr_day = events.withColumn('current_date',F.current_date())
# events_curr_day.show(10, False)
# events_diff = events_curr_day\
# .withColumn('date', F.lit('2022-05-31'))\
# .withColumn('diff',F.datediff(F.col('current_date'),F.col('date')))
# events_diff.show(10, False)
# 2. Добавляем колонки с часами, минутами и секундами


# 3. Сортируем по event.datetime по убыванию
result = events \
    .withColumn("hour", F.hour("event.datetime")) \
    .withColumn("minute", F.minute("event.datetime")) \
    .withColumn("second", F.second("event.datetime")) \

# 4. Проверяем результат
result.orderBy(F.col("event.datetime").desc()).show(10, True)
