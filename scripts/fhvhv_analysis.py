from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round, hour, desc
import time

spark = SparkSession.builder.appName("FHVHVAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("s3://lab03-indice-invertido/fhvhv_partitioned/")

print("=== 1. Total viajes por año ===")
t = time.time()
df.groupBy("year").count().orderBy("year").show()
print(f"Tiempo: {time.time()-t:.2f} seg\n")

print("=== 2. Promedio distancia y tiempo por año ===")
t = time.time()
df.groupBy("year").agg(
    round(avg("trip_miles"), 2).alias("promedio_millas"),
    round(avg("trip_time")/60, 2).alias("promedio_minutos")
).orderBy("year").show()
print(f"Tiempo: {time.time()-t:.2f} seg\n")

print("=== 3. Horas pico 2025 ===")
t = time.time()
df.filter(col("year") == 2025) \
  .withColumn("hora", hour(col("pickup_datetime"))) \
  .groupBy("hora").count().orderBy(desc("count")).show(5)
print(f"Tiempo: {time.time()-t:.2f} seg\n")

print("=== 4. Promedio propina y pago conductor por año ===")
t = time.time()
df.groupBy("year").agg(
    round(avg("tips"), 2).alias("promedio_propina"),
    round(avg("driver_pay"), 2).alias("promedio_conductor")
).orderBy("year").show()
print(f"Tiempo: {time.time()-t:.2f} seg\n")

print("=== 5. SIN filtro de particion (lee todo) ===")
t = time.time()
total = df.count()
print(f"Total: {total} — Tiempo: {time.time()-t:.2f} seg\n")

print("=== 6. CON filtro de particion (solo 2026) ===")
t = time.time()
total_2026 = df.filter(col("year") == 2026).count()
print(f"Total 2026: {total_2026} — Tiempo: {time.time()-t:.2f} seg\n")

spark.stop()
