import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()
events = spark.read.json("/user/master/data/events/date=2022-05-25").cache()
# events.filter(F.col('event.message_to').isNotNull()).count()
# events.count() - events.na.drop(subset='event.message_from').count()

# 2. Фильтруем только реакции
reactions = events.filter(F.col("event_type") == "reaction")

# 3. Группируем по reaction_from и считаем количество
counts = reactions.groupBy("event.reaction_from").count()

# 4. Находим максимум
counts.agg(F.max("count")).show()
