from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, col
from pyspark.sql.functions import input_file_name, collect_set
import time

spark = SparkSession.builder \
    .appName("InvertedIndexSpark") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

t = time.time()

df = spark.read.text(
    "s3://lab03-indice-invertido/wiki_combined_large/"
).withColumn(
    "doc",
    input_file_name()
)

words = df.select(
    explode(
        split(lower(col("value")), "[^a-z]+")
    ).alias("word"),
    col("doc")
)

index_df = (
    words
    .filter(col("word") != "")
    .groupBy("word")
    .agg(collect_set("doc").alias("documents"))
)

index_df.show(20, truncate=False)

print(f"Tiempo: {time.time()-t:.2f} seg")

spark.stop()
