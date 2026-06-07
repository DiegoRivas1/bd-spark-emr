from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, col, length
import time

spark = SparkSession.builder \
    .appName("WordCountLarge") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

t = time.time()

# Leer los archivos combinados
df = spark.read.text(
    "s3://lab03-indice-invertido/wiki_combined_large/"
)

result = (
    df.select(
        explode(
            split(lower(col("value")), "[^a-z]+")
        ).alias("word")
    )
    .filter(
        (col("word") != "") &
        (length(col("word")) > 2)
    )
    .groupBy("word")
    .count()
    .orderBy(col("count").desc())
)

result.show(20, truncate=False)

print(f"Tiempo: {time.time()-t:.2f} seg")

spark.stop()

