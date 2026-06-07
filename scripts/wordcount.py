from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, col, length

spark = SparkSession.builder.appName("WordCount").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Leer directo de S3
df = spark.read.text("s3://lab03-indice-invertido/input/")

# WordCount
words = df.select(
    explode(split(lower(col("value")), "[^a-z]+")).alias("word")
).filter(
    (col("word") != "") & (length(col("word")) > 2)
)

result = words.groupBy("word").count().orderBy("count", ascending=False)
result.show(20)

spark.stop()
