from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round, hour, desc
import time

spark = SparkSession.builder.appName("TaxiAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("s3://lab03-indice-invertido/taxi_partitioned/")

print("=== 1. Total viajes por año ===")
t = time.time()
df.groupBy("year").count().orderBy("year").show()
print(f"Tiempo: {time.time()-t:.2f} seg\n")

print("=== 2. Promedio distancia por año ===")
t = time.time()
df.groupBy("year").agg(round(avg("trip_distance"), 2).alias("promedio_millas")).orderBy("year").show()
print(f"Tiempo: {time.time()-t:.2f} seg\n")

print("=== 3. Horas pico 2025 ===")
t = time.time()
df.filter(col("year") == 2025) \
  .withColumn("hora", hour(col("tpep_pickup_datetime"))) \
  .groupBy("hora").count().orderBy(desc("count")).show(5)
print(f"Tiempo: {time.time()-t:.2f} seg\n")

print("=== 4. Metodos de pago ===")
t = time.time()
df.groupBy("payment_type").count().orderBy(desc("count")).show()
print(f"Tiempo: {time.time()-t:.2f} seg\n")

print("=== 5. Promedio propina por metodo de pago ===")
t = time.time()
df.groupBy("payment_type").agg(round(avg("tip_amount"), 2).alias("promedio_propina")).orderBy(desc("promedio_propina")).show()
print(f"Tiempo: {time.time()-t:.2f} seg\n")

print("=== 6. SIN filtro de particion (lee todo) ===")
t = time.time()
df.count()
print(f"Total: {df.count()} — Tiempo: {time.time()-t:.2f} seg\n")

print("=== 7. CON filtro de particion (solo 2026) ===")
t = time.time()
count_2026 = df.filter(col("year") == 2026).count()
print(f"Total 2026: {count_2026} — Tiempo: {time.time()-t:.2f} seg\n")

spark.stop()
