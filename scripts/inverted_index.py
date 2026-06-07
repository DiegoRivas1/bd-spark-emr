from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, col, length, input_file_name, basename, collect_set

spark = SparkSession.builder.appName("InvertedIndex").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Leer con nombre de archivo
df = spark.read.text("s3://lab03-indice-invertido/input/") \
    .withColumn("filename", basename(input_file_name()))

# Índice invertido
words = df.select(
    explode(split(lower(col("value")), "[^a-z]+")).alias("word"),
    col("filename")
).filter(
    (col("word") != "") & (length(col("word")) > 2)
)

result = words.groupBy("word") \
    .agg(collect_set("filename").alias("documentos")) \
    .orderBy("word")

result.show(20, truncate=False)

spark.stop()
